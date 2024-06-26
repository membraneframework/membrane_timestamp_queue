defmodule Membrane.TimestampQueue do
  @moduledoc """
  Implementation of a queue, that accepts:
   - Membrane buffers
   - events
   - stream formats
   - end of streams
  from various pads. Items in queue are sorted according to their timestamps.

  Moreover, #{inspect(__MODULE__)} is able to manage demand of pads, based on the amount of buffers
  from each pad currently stored in the queue.
  """

  use Bunch.Access

  alias Membrane.{Buffer, Event, Pad, StreamFormat}
  alias Membrane.Element.Action

  @typep pad_queue :: %{
           timestamp_offset: integer(),
           qex: Qex.t(),
           buffers_size: non_neg_integer(),
           buffers_number: non_neg_integer(),
           end_of_stream?: boolean(),
           use_pts?: boolean() | nil,
           max_timestamp_on_qex: Membrane.Time.t() | nil,
           timestamps_qex: Qex.t() | nil
         }

  @typedoc """
  A queue, that accepts buffers, stream formats and events from various pads and sorts them based on
  their timestamps.
  """
  @opaque t :: %__MODULE__{
            current_queue_time: Membrane.Time.t(),
            pause_demand_boundary: pos_integer() | :infinity,
            metric_unit: :buffers | :bytes | :time,
            pad_queues: %{optional(Pad.ref()) => pad_queue()},
            pads_heap: Heap.t(),
            blocking_registered_pads: MapSet.t(Pad.ref()),
            registered_pads_offsets: %{optional(Pad.ref()) => integer()},
            # :awaiting_pads contain at most one element at the time
            awaiting_pads: [Pad.ref()],
            closed?: boolean(),
            chunk_duration: nil | Membrane.Time.t(),
            chunk_full?: boolean(),
            next_chunk_boundary: nil | Membrane.Time.t(),
            synchronization_strategy: :synchronize_on_arrival | :explicit_offsets,
            known_pads: MapSet.t(Pad.ref())
          }

  defstruct current_queue_time: Membrane.Time.seconds(0),
            pause_demand_boundary: :infinity,
            metric_unit: :buffers,
            pad_queues: %{},
            pads_heap: Heap.max(),
            blocking_registered_pads: MapSet.new(),
            registered_pads_offsets: %{},
            awaiting_pads: [],
            closed?: false,
            chunk_duration: nil,
            chunk_full?: false,
            next_chunk_boundary: nil,
            synchronization_strategy: :synchronize_on_arrival,
            known_pads: MapSet.new()

  @typedoc """
  Value passed to `:pause_demand_boundary` option in `new/1`.

  Specyfies, what amount of buffers associated with a specific pad must be stored in the queue, to pause auto demand.

  Is a two-element tuple, which
    - the first element specifies metric, in which boundary is expressed (default to `:buffers`)
    - the second element is the boundary (default to `1000`).
  """
  @type pause_demand_boundary ::
          {:buffers | :bytes, pos_integer() | :infinity} | {:time, Membrane.Time.t()}

  @typedoc """
  Options passed to `#{inspect(__MODULE__)}.new/1`.

  Following options are allowed:
    - `:pause_demand_boundary` - `t:pause_demand_boundary/0`. Defaults to `{:buffers, 1000}`.
    - `:chunk_duration` - `t:Membrane.Time.t/0`. Specifies how long the fragments returned by
      `#{inspect(__MODULE__)}.pop_chunked/1` will be approximately. If not set, popping chunks will not be available.
    - `:synchronization_strategy` - `:synchronize_on_arrival` or `:exact_timestamps` (default to `:synchronize_on_arrival`).
      Specyfies, how items from different pads will be synchronized with each other. If it is set to:
      * `:synchronize_on_arrival` - in the moment of the arrival of the first buffer from a specific pad, there will be
        caluclated timestamp offset for this pad. These offsets will be added to the buffers timestamps, to caluclate from
        which pad items should be returned in the first order. Every offset will be calculated in such a way that the first
        buffer from a new pad will be returned as the next item.
      * `:explicit_offsets` - buffers from various pads will be sorted based on their timestamps and pads offsets. Pads
        offsets can be set using `#{inspect(__MODULE__)}.register_pad/3` function. If pad offset is not explicitly set
        before the first buffer from this pad, it will be equal 0.
  """
  @type options :: [
          pause_demand_boundary: pause_demand_boundary(),
          chunk_duration: Membrane.Time.t(),
          synchronization_strategy: :synchronize_on_arrival | :explicit_offsets
        ]

  @spec new(options) :: t()
  def new(options \\ []) do
    [
      chunk_duration: chunk_duration,
      pause_demand_boundary: {unit, boundary},
      synchronization_strategy: synchronization_strategy
    ] =
      options
      |> Keyword.validate!(
        chunk_duration: nil,
        pause_demand_boundary: {:buffers, 1000},
        synchronization_strategy: :synchronize_on_arrival
      )
      |> Enum.sort()

    %__MODULE__{
      pause_demand_boundary: boundary,
      metric_unit: unit,
      chunk_duration: chunk_duration,
      synchronization_strategy: synchronization_strategy
    }
  end

  @typedoc """
  Options passed to `#{inspect(__MODULE__)}.register_pad/3`.

  Following options are allowed:
    - `:wait_on_buffers?` - `boolean()`, default to `true`. Specyfies, if the queue will wait with returning buffers
      in `pop_*` functions, until it receives the first buffer from a pad passed as a second argument to the function.
    - `:timestamp_offset` - integer. Specyfies, what will be the timestamp offset of a pad passed as a second argument
      to the function. Allowed only if `#{inspect(__MODULE__)}` synchronization strategy is `:explicit_offsets`.
  """
  @type register_pad_options :: [
          timestamp_offset: integer(),
          wait_on_buffers?: boolean()
        ]

  @doc """
  Registers an input pad in the queue without pushing anything on that pad.

  Once a pad is registered with option `wait_on_buffers?: true` (default), the `pop_available_items/3` function won't
  return any buffers until a `buffer` or `end_of_stream` is available on the registered pad.

  Pushing a buffer on an unregistered pad automatically registers it.
  """
  @spec register_pad(t(), Pad.ref(), register_pad_options()) :: t()
  def register_pad(%__MODULE__{} = timestamp_queue, pad_ref, opts \\ []) do
    [timestamp_offset: offset, wait_on_buffers?: wait?] =
      opts
      |> Keyword.validate!(timestamp_offset: nil, wait_on_buffers?: true)
      |> Enum.sort()

    if offset != nil and timestamp_queue.synchronization_strategy == :synchronize_on_arrival do
      raise """
      Option :timestamp_offset in #{inspect(__MODULE__)}.register_pad/3 cannot be set if #{inspect(__MODULE__)} \
      synchronization strategy is :synchronize_on_arrival (default).
      """
    end

    with %{timestamp_offset: offset} when offset != nil <-
           Map.get(timestamp_queue.pad_queues, pad_ref) do
      raise """
      Cannot register pad `#{inspect(pad_ref)}, because buffers from it are already in `#{inspect(__MODULE__)}. \
      Every pad can be registered only before pushing the first buffer from it on the queue.
      """
    end

    timestamp_queue =
      if(wait?, do: [:blocking_registered_pads, :known_pads], else: [:known_pads])
      |> Enum.reduce(timestamp_queue, fn field_name, timestamp_queue ->
        Map.update!(timestamp_queue, field_name, &MapSet.put(&1, pad_ref))
      end)

    if offset != nil,
      do: put_in(timestamp_queue, [:registered_pads_offsets, pad_ref], offset),
      else: timestamp_queue
  end

  @doc """
  Pushes a buffer associated with a specified pad to the queue.

  Returns a suggested actions list and the updated queue.

  If the amount of buffers associated with the specified pad in the queue just exceded
  `pause_demand_boundary`, the suggested actions list contains `t:Membrane.Action.pause_auto_demand()`
  action, otherwise it is equal an empty list.

  Buffers pushed to the queue must have a non-`nil` `dts` or `pts`.
  """
  @spec push_buffer(t(), Pad.ref(), Buffer.t()) :: {[Action.pause_auto_demand()], t()}
  def push_buffer(_timestamp_queue, pad_ref, %Buffer{dts: nil, pts: nil} = buffer) do
    raise """
    #{inspect(__MODULE__)} accepts only buffers whose dts or pts is not nil, but it received\n#{inspect(buffer, pretty: true)}
    from pad #{inspect(pad_ref)}
    """
  end

  def push_buffer(%__MODULE__{} = timestamp_queue, pad_ref, buffer) do
    {actions, timestamp_queue} =
      timestamp_queue
      |> Map.update!(:pad_queues, &Map.put_new_lazy(&1, pad_ref, fn -> new_pad_queue() end))
      |> get_and_update_in([:pad_queues, pad_ref], fn pad_queue ->
        old_buffers_size = pad_queue.buffers_size

        pad_queue =
          pad_queue
          |> Map.update!(:buffers_number, &(&1 + 1))
          |> maybe_handle_first_buffer(pad_ref, buffer, timestamp_queue)
          |> increase_buffers_size(buffer, timestamp_queue.metric_unit)
          |> check_timestamps_consistency!(buffer, pad_ref)

        boundary = timestamp_queue.pause_demand_boundary

        actions =
          if pad_queue.buffers_size >= boundary and old_buffers_size < boundary,
            do: [pause_auto_demand: pad_ref],
            else: []

        {actions, pad_queue}
      end)

    pad_queue = timestamp_queue.pad_queues |> Map.get(pad_ref)
    buff_time = buffer_time(buffer, pad_queue)

    timestamp_queue =
      timestamp_queue
      |> Map.update!(:next_chunk_boundary, fn
        nil when timestamp_queue.chunk_duration != nil ->
          buff_time + timestamp_queue.chunk_duration

        other ->
          other
      end)
      |> Map.update!(:known_pads, &MapSet.put(&1, pad_ref))
      |> remove_pad_from_registered_and_awaiting_pads(pad_ref)
      |> push_pad_on_heap_if_qex_empty(pad_ref, -buff_time, pad_queue)
      |> push_item_on_qex(pad_ref, {:buffer, buffer})

    {actions, timestamp_queue}
  end

  defp maybe_handle_first_buffer(
         %{timestamp_offset: nil} = pad_queue,
         pad_ref,
         first_buffer,
         timestamp_queue
       ) do
    offset =
      case timestamp_queue.synchronization_strategy do
        :synchronize_on_arrival ->
          (first_buffer.dts || first_buffer.pts) - timestamp_queue.current_queue_time

        :explicit_offsets ->
          Map.get(timestamp_queue.registered_pads_offsets, pad_ref, 0)
      end

    use_pts? = first_buffer.dts == nil

    %{pad_queue | timestamp_offset: offset, use_pts?: use_pts?}
  end

  defp maybe_handle_first_buffer(pad_queue, _pad_ref, _buffer, _timestamp_queue), do: pad_queue

  defp check_timestamps_consistency!(pad_queue, buffer, pad_ref) do
    if not pad_queue.use_pts? and buffer.dts == nil do
      raise """
      Buffer #{inspect(buffer, pretty: true)} from pad #{inspect(pad_ref)} has nil dts, while \
      the first buffer from this pad had valid integer there. If the first buffer from a pad has \
      dts different from nil, all later buffers from this pad must meet this property.
      """
    end

    buffer_timestamp = if pad_queue.use_pts?, do: buffer.pts, else: buffer.dts
    max_timestamp = pad_queue.max_timestamp_on_qex

    if is_integer(max_timestamp) and max_timestamp > buffer_timestamp do
      timestamp_field = if pad_queue.use_pts?, do: "pts", else: "dts"

      raise """
      Buffer #{inspect(buffer, pretty: true)} from pad #{inspect(pad_ref)} has #{timestamp_field} equal \
      #{inspect(buffer_timestamp)}, but previous buffer pushed on queue from this pad had #{timestamp_field} \
      equal #{inspect(max_timestamp)}. Buffers from a single pad must have non-decreasing timestamps.
      """
    end

    %{pad_queue | max_timestamp_on_qex: buffer_timestamp}
  end

  @doc """
  Pushes stream format associated with a specified pad to the queue.

  Returns the updated queue.
  """
  @spec push_stream_format(t(), Pad.ref(), StreamFormat.t()) :: t()
  def push_stream_format(%__MODULE__{} = timestamp_queue, pad_ref, stream_format) do
    push_item(timestamp_queue, pad_ref, {:stream_format, stream_format})
  end

  @doc """
  Pushes event associated with a specified pad to the queue.

  Returns the updated queue.
  """
  @spec push_event(t(), Pad.ref(), Event.t()) :: t()
  def push_event(%__MODULE__{} = timestamp_queue, pad_ref, event) do
    push_item(timestamp_queue, pad_ref, {:event, event})
  end

  @doc """
  Pushes end of stream of the specified pad to the queue.

  Returns the updated queue.
  """
  @spec push_end_of_stream(t(), Pad.ref()) :: t()
  def push_end_of_stream(%__MODULE__{} = timestamp_queue, pad_ref) do
    timestamp_queue
    |> push_item(pad_ref, :end_of_stream)
    |> put_in([:pad_queues, pad_ref, :end_of_stream?], true)
    |> remove_pad_from_registered_and_awaiting_pads(pad_ref)
  end

  defp remove_pad_from_registered_and_awaiting_pads(timestamp_queue, pad_ref) do
    timestamp_queue
    |> Map.update!(:blocking_registered_pads, &MapSet.delete(&1, pad_ref))
    |> Map.update!(:registered_pads_offsets, &Map.delete(&1, pad_ref))
    |> Map.update!(:awaiting_pads, &List.delete(&1, pad_ref))
  end

  defp push_item(%__MODULE__{} = timestamp_queue, pad_ref, item) do
    timestamp_queue
    |> Map.update!(:pad_queues, &Map.put_new_lazy(&1, pad_ref, fn -> new_pad_queue() end))
    |> Map.update!(:known_pads, &MapSet.put(&1, pad_ref))
    |> push_pad_on_heap_if_qex_empty(pad_ref, :infinity)
    |> push_item_on_qex(pad_ref, item)
  end

  defp new_pad_queue() do
    %{
      timestamp_offset: nil,
      qex: Qex.new(),
      buffers_size: 0,
      buffers_number: 0,
      end_of_stream?: false,
      use_pts?: nil,
      max_timestamp_on_qex: nil,
      timestamps_qex: nil
    }
  end

  defp increase_buffers_size(pad_queue, %Buffer{} = buffer, :time) do
    new_last_timestamp = buffer_time(buffer, pad_queue)

    pad_queue =
      with %{timestamps_qex: nil} <- pad_queue do
        %{pad_queue | timestamps_qex: Qex.new()}
      end

    case Qex.last(pad_queue.timestamps_qex) do
      {:value, old_last_timestamp} ->
        time_interval = new_last_timestamp - old_last_timestamp
        %{pad_queue | buffers_size: pad_queue.buffers_size + time_interval}

      :empty ->
        pad_queue
    end
    |> Map.update!(:timestamps_qex, &Qex.push(&1, new_last_timestamp))
  end

  defp increase_buffers_size(pad_queue, _buffer, :buffers),
    do: %{pad_queue | buffers_size: pad_queue.buffers_size + 1}

  defp increase_buffers_size(pad_queue, %Buffer{payload: payload}, :bytes),
    do: %{pad_queue | buffers_size: pad_queue.buffers_size + byte_size(payload)}

  defp decrease_buffers_size(pad_queue, _buffer, :time) do
    {first_timestamp, timestamps_qex} = Qex.pop!(pad_queue.timestamps_qex)
    pad_queue = %{pad_queue | timestamps_qex: timestamps_qex}

    case Qex.first(timestamps_qex) do
      {:value, second_timestamp} ->
        time_interval = second_timestamp - first_timestamp
        %{pad_queue | buffers_size: pad_queue.buffers_size - time_interval}

      :empty ->
        pad_queue
    end
  end

  defp decrease_buffers_size(pad_queue, _buffer, :buffers),
    do: %{pad_queue | buffers_size: pad_queue.buffers_size - 1}

  defp decrease_buffers_size(pad_queue, %Buffer{payload: payload}, :bytes),
    do: %{pad_queue | buffers_size: pad_queue.buffers_size - byte_size(payload)}

  defp buffer_time(%Buffer{dts: dts}, %{use_pts?: false, timestamp_offset: timestamp_offset}),
    do: dts - timestamp_offset

  defp buffer_time(%Buffer{pts: pts}, %{use_pts?: true, timestamp_offset: timestamp_offset}),
    do: pts - timestamp_offset

  @type item ::
          {:stream_format, StreamFormat.t()}
          | {:buffer, Buffer.t()}
          | {:event, Event.t()}
          | :end_of_stream

  @type popped_value :: {Pad.ref(), item()}

  @doc """
  Pops items from the queue while they are available.

  A buffer `b` from pad `p` is available, if all pads different than `p`
    - either have a buffer in the queue, that is older than `b`
    - or haven't ever had any buffer on the queue
    - or have end of stream pushed on the queue.

  An item other than a buffer is considered available if all newer buffers on the same pad are
  available.

  The returned value is a suggested actions list, a list of popped items and the updated queue.

  If the amount of buffers associated with any pad in the queue falls below the
  `pause_demand_boundary`, the suggested actions list contains `t:Membrane.Action.resume_auto_demand()`
  actions, otherwise it is an empty list.
  """
  @spec pop_available_items(t()) :: {[Action.resume_auto_demand()], [popped_value()], t()}
  def pop_available_items(%__MODULE__{} = timestamp_queue) do
    {actions, items, timestamp_queue} = do_pop(timestamp_queue, [], [], false)

    timestamp_queue =
      with %{chunk_duration: chunk_duration} when chunk_duration != nil <- timestamp_queue do
        %{
          timestamp_queue
          | next_chunk_boundary: timestamp_queue.current_queue_time + chunk_duration
        }
      end

    {actions, items, timestamp_queue}
  end

  @doc """
  The equivalent of calling `push_buffer/2` and then `pop_available_items/1`.
  """
  @spec push_buffer_and_pop_available_items(t(), Pad.ref(), Buffer.t()) ::
          {[Action.pause_auto_demand() | Action.resume_auto_demand()], [popped_value()], t()}
  def push_buffer_and_pop_available_items(%__MODULE__{} = timestamp_queue, pad_ref, buffer) do
    push_buffer_and_pop(timestamp_queue, pad_ref, buffer, &pop_available_items/1)
  end

  @type chunk :: [popped_value()]

  @doc """
  Works like `pop_available_items/1`, but the returned items are arranged in chunks of duration `chunk_duration`.

  `chunk_duration` must be passed as an option to `new/1`. The duration of each chunk may not be exactly the
  `chunk_duration`, but the average duration will converge to it. With that exception, only full chunks are
  returned.

  See `pop_available_items/1` for details.
  """
  @spec pop_chunked(t()) :: {[Action.resume_auto_demand()], [chunk()], t()}
  def pop_chunked(%__MODULE__{chunk_duration: nil}) do
    raise """
    Cannot invoke function #{inspect(__MODULE__)}.pop_chunked/1 on a queue, that has :chunk_duration field \
    set to nil. You can set this field by passing {:chunk_duration, some_membrane_time} option to \
    #{inspect(__MODULE__)}.new/1.
    """
  end

  def pop_chunked(%__MODULE__{} = timestamp_queue) do
    min_max_time =
      timestamp_queue.pad_queues
      |> Enum.reduce(:infinity, fn
        {_pad_ref, %{end_of_stream?: true}}, min_max_time ->
          min_max_time

        {_pad_ref, %{max_timestamp_on_qex: max_ts, timestamp_offset: offset}}, min_max_time ->
          min(min_max_time, max_ts - offset)
      end)

    do_pop_chunked(timestamp_queue, min_max_time)
  end

  defp do_pop_chunked(timestamp_queue, min_max_time) do
    if min_max_time >= timestamp_queue.next_chunk_boundary and
         (min_max_time != :infinity or timestamp_queue.pad_queues != %{}) do
      {actions, chunk, timestamp_queue} = do_pop(timestamp_queue, [], [], true)

      {next_actions, chunks, timestamp_queue} =
        %{timestamp_queue | chunk_full?: false}
        |> Map.update!(:next_chunk_boundary, &(&1 + timestamp_queue.chunk_duration))
        |> do_pop_chunked(min_max_time)

      {actions ++ next_actions, [chunk] ++ chunks, timestamp_queue}
    else
      {[], [], timestamp_queue}
    end
  end

  @doc """
  The equivalent of calling `push_buffer/2` and then `pop_chunked/1`.
  """
  @spec push_buffer_and_pop_chunked(t(), Pad.ref(), Buffer.t()) ::
          {[Action.pause_auto_demand() | Action.resume_auto_demand()], [chunk()], t()}
  def push_buffer_and_pop_chunked(%__MODULE__{} = timestamp_queue, pad_ref, buffer) do
    push_buffer_and_pop(timestamp_queue, pad_ref, buffer, &pop_chunked/1)
  end

  defp do_pop(%__MODULE__{} = timestamp_queue, actions_acc, items_acc, pop_chunk?) do
    try_return_buffer? =
      MapSet.size(timestamp_queue.blocking_registered_pads) == 0 and
        timestamp_queue.awaiting_pads == [] and
        not timestamp_queue.chunk_full?

    case Heap.root(timestamp_queue.pads_heap) do
      {priority, pad_ref} when try_return_buffer? or priority == :infinity ->
        {actions, items, timestamp_queue} =
          timestamp_queue
          |> Map.update!(:pads_heap, &Heap.pop/1)
          |> pop_buffer_and_following_items(pad_ref, pop_chunk?)

        do_pop(timestamp_queue, actions ++ actions_acc, items ++ items_acc, pop_chunk?)

      _other ->
        {actions_acc, Enum.reverse(items_acc), timestamp_queue}
    end
  end

  @spec pop_buffer_and_following_items(t(), Pad.ref(), boolean()) ::
          {[Action.resume_auto_demand()], [popped_value()], t()}
  defp pop_buffer_and_following_items(%__MODULE__{} = timestamp_queue, pad_ref, pop_chunk?) do
    pad_queue = timestamp_queue.pad_queues |> Map.get(pad_ref)

    {actions, items, timestamp_queue} =
      with {{:value, {:buffer, buffer}}, qex} <- Qex.pop(pad_queue.qex),
           buffer_time when not pop_chunk? or buffer_time < timestamp_queue.next_chunk_boundary <-
             buffer_time(buffer, pad_queue) do
        old_buffers_size = pad_queue.buffers_size

        pad_queue =
          %{pad_queue | qex: qex, buffers_number: pad_queue.buffers_number - 1}
          |> decrease_buffers_size(buffer, timestamp_queue.metric_unit)

        timestamp_queue =
          with %{buffers_number: 0, end_of_stream?: false} <- pad_queue do
            Map.update!(timestamp_queue, :awaiting_pads, &[pad_ref | &1])
          else
            _pad_queue -> timestamp_queue
          end

        timestamp_queue =
          %{timestamp_queue | current_queue_time: buffer_time}
          |> put_in([:pad_queues, pad_ref], pad_queue)

        boundary = timestamp_queue.pause_demand_boundary

        actions =
          if pad_queue.buffers_size < boundary and old_buffers_size >= boundary,
            do: [resume_auto_demand: pad_ref],
            else: []

        items = [{pad_ref, {:buffer, buffer}}]

        {actions, items, timestamp_queue}
      else
        buffer_time when is_integer(buffer_time) and pop_chunk? ->
          {[], [], %{timestamp_queue | chunk_full?: true}}

        _non_buffer_pop_result ->
          {[], [], timestamp_queue}
      end

    pop_following_items(timestamp_queue, pad_ref, actions, items)
  end

  @spec pop_following_items(t(), Pad.ref(), [Action.resume_auto_demand()], [popped_value()]) ::
          {[Action.resume_auto_demand()], [popped_value()], t()}
  defp pop_following_items(%__MODULE__{} = timestamp_queue, pad_ref, actions_acc, items_acc) do
    pad_queue = timestamp_queue.pad_queues |> Map.get(pad_ref)

    case Qex.pop(pad_queue.qex) do
      {{:value, {:buffer, buffer}}, _qex} ->
        new_priority = -buffer_time(buffer, pad_queue)
        timestamp_queue = push_pad_on_heap(timestamp_queue, pad_ref, new_priority)

        {actions_acc, items_acc, timestamp_queue}

      {{:value, item}, qex} ->
        timestamp_queue = put_in(timestamp_queue, [:pad_queues, pad_ref, :qex], qex)
        items_acc = [{pad_ref, item}] ++ items_acc

        pop_following_items(timestamp_queue, pad_ref, actions_acc, items_acc)

      {:empty, _empty_qex} when timestamp_queue.awaiting_pads == [pad_ref] ->
        {actions_acc, items_acc, timestamp_queue}

      {:empty, _empty_qex} ->
        {pad_queue, timestamp_queue} = pop_in(timestamp_queue, [:pad_queues, pad_ref])

        timestamp_queue =
          if pad_queue.end_of_stream?,
            do: Map.update!(timestamp_queue, :known_pads, &MapSet.delete(&1, pad_ref)),
            else: timestamp_queue

        {actions_acc, items_acc, timestamp_queue}
    end
  end

  defp push_buffer_and_pop(timestamp_queue, pad_ref, buffer, pop_fun) do
    {maybe_pause, timestamp_queue} = push_buffer(timestamp_queue, pad_ref, buffer)
    {maybe_resume, items, timestamp_queue} = pop_fun.(timestamp_queue)

    actions =
      with [pause_auto_demand: pad_ref] <- maybe_pause,
           index when is_integer(index) <-
             Enum.find_index(maybe_resume, &(&1 == {:resume_auto_demand, pad_ref})) do
        List.delete_at(maybe_resume, index)
      else
        _other -> maybe_pause ++ maybe_resume
      end

    {actions, items, timestamp_queue}
  end

  defp push_item_on_qex(timestamp_queue, pad_ref, item) do
    :ok = ensure_queue_not_closed!(timestamp_queue, pad_ref, item)

    timestamp_queue
    |> update_in([:pad_queues, pad_ref, :qex], &Qex.push(&1, item))
  end

  defp push_pad_on_heap_if_qex_empty(timestamp_queue, pad_ref, priority, pad_queue \\ nil) do
    qex =
      (pad_queue || Map.get(timestamp_queue.pad_queues, pad_ref))
      |> Map.get(:qex)

    if qex == Qex.new(),
      do: push_pad_on_heap(timestamp_queue, pad_ref, priority),
      else: timestamp_queue
  end

  defp push_pad_on_heap(timestamp_queue, pad_ref, priority) do
    heap_item = {priority, pad_ref}
    Map.update!(timestamp_queue, :pads_heap, &Heap.push(&1, heap_item))
  end

  @doc """
  Pops all items in the proper order and closes the queue.

  After being closed, nothing can be pushed to the queue anymore - a new queue should be created if
  needed.

  The returned value is a suggested actions list, a list of popped buffers and the updated queue.

  Suggested actions list contains `t:Membrane.Action.resume_auto_demand()` for every pad, that had
  pasued auto demand before the flush.
  """
  @spec flush_and_close(t()) :: {[Action.resume_auto_demand()], [popped_value()], t()}
  def flush_and_close(%__MODULE__{} = timestamp_queue) do
    %{timestamp_queue | closed?: true, blocking_registered_pads: MapSet.new(), awaiting_pads: []}
    |> Map.update!(
      :pad_queues,
      &Map.new(&1, fn {pad_ref, data} ->
        {pad_ref, %{data | end_of_stream?: true}}
      end)
    )
    |> pop_available_items()
  end

  @doc """
  Returns true, if the pad has been registered in the queue or item from it has been pushed to the queue
  and moreover, end of stream of this pad hasn't been popped from the queue.
  """
  @spec has_pad?(t(), Pad.ref()) :: boolean()
  def has_pad?(%__MODULE__{} = timestamp_queue, pad_ref) do
    MapSet.member?(timestamp_queue.known_pads, pad_ref)
  end

  @doc """
  Returns `t:MapSet.t/0` of all pads, that:
    1) have been ever registered in the queue or item from them has been pushed to the queue
    2) their end of stream hasn't been popped from the queue.
  """
  @spec pads(t()) :: MapSet.t(Pad.ref())
  def pads(%__MODULE__{} = timestamp_queue) do
    timestamp_queue.known_pads
  end

  defp ensure_queue_not_closed!(%__MODULE__{closed?: true}, pad_ref, item) do
    inspected_item =
      case item do
        :end_of_stream -> "end of stream"
        {:stream_format, value} -> "stream format #{inspect(value)}"
        {type, value} -> "#{type} #{inspect(value)}"
      end

    raise """
    Unable to push #{inspected_item} from pad #{inspect(pad_ref)} on the already closed #{inspect(__MODULE__)}. \
    After calling #{inspect(__MODULE__)}.flush_and_close/1 queue is not capable to handle new items and new \
    queue has to be created.
    """
  end

  defp ensure_queue_not_closed!(_timestamp_queue, _pad_ref, _item), do: :ok
end
