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
            blocking_registered_pads: MapSet.t(),
            registered_pads_offsets: %{optional(Pad.ref()) => integer()},
            awaiting_pads: [Pad.ref()],
            closed?: boolean(),
            chunk_duration: nil | Membrane.Time.t(),
            chunk_full?: boolean(),
            next_chunk_boundary: nil | Membrane.Time.t(),
            max_time_in_queues: Membrane.Time.t(),
            synchronization_strategy: :synchronize_on_arrival | :explicit_offsets
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
            max_time_in_queues: 0,
            synchronization_strategy: :synchronize_on_arrival

  @typedoc """
  Options passed to `#{inspect(__MODULE__)}.new/1`.

  Following options are allowed:
    - `:pause_demand_boundary` - positive integer, `t:Membrane.Time.t()` or `:infinity` (default to `:infinity`). Tells,
      what amount of buffers associated with specific pad must be stored in the queue, to pause auto demand.
    - `:pause_demand_boundary_unit` - `:buffers`, `:bytes` or `:time` (deafult to `:buffers`). Tells, in which metric
      `:pause_demand_boundary` is specified.
    - `:chunk_duration` - `Membrane.Time.t()`. Specifies how long the fragments returned by
      `#{inspect(__MODULE__)}.pop_chunked/1` will be approximately.
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
          pause_demand_boundary: pos_integer() | Membrane.Time.t() | :infinity,
          pause_demand_boundary_unit: :buffers | :bytes | :time,
          chunk_duration: Membrane.Time.t(),
          synchronization_strategy: :synchronize_on_arrival | :explicit_offsets
        ]

  @spec new(options) :: t()
  def new(options \\ []) do
    [
      chunk_duration: chunk_duration,
      pause_demand_boundary: boundary,
      pause_demand_boundary_unit: unit,
      synchronization_strategy: synchronization_strategy
    ] =
      options
      |> Keyword.validate!(
        chunk_duration: nil,
        pause_demand_boundary: :infinity,
        pause_demand_boundary_unit: :buffers,
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
      if wait?,
        do: Map.update!(timestamp_queue, :blocking_registered_pads, &MapSet.put(&1, pad_ref)),
        else: timestamp_queue

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
      |> push_item(pad_ref, {:buffer, buffer})
      |> Map.update!(:blocking_registered_pads, &MapSet.delete(&1, pad_ref))
      |> Map.update!(:awaiting_pads, &List.delete(&1, pad_ref))
      |> get_and_update_in([:pad_queues, pad_ref], fn pad_queue ->
        old_buffers_size = pad_queue.buffers_size

        pad_queue =
          pad_queue
          |> Map.update!(:buffers_number, &(&1 + 1))
          |> Map.update!(:timestamp_offset, fn
            nil when timestamp_queue.synchronization_strategy == :synchronize_on_arrival ->
              (buffer.dts || buffer.pts) - timestamp_queue.current_queue_time

            nil when timestamp_queue.synchronization_strategy == :explicit_offsets ->
              Map.get(timestamp_queue.registered_pads_offsets, pad_ref, 0)

            valid_offset ->
              valid_offset
          end)
          |> Map.update!(:use_pts?, fn
            nil -> buffer.dts == nil
            valid_boolean -> valid_boolean
          end)
          |> Map.update!(:timestamps_qex, fn
            nil when timestamp_queue.metric_unit == :time -> Qex.new()
            other -> other
          end)
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
      |> Map.update!(:max_time_in_queues, &max(&1, buff_time))
      |> Map.update!(:next_chunk_boundary, fn
        nil when timestamp_queue.chunk_duration != nil ->
          buff_time + timestamp_queue.chunk_duration

        other ->
          other
      end)
      |> Map.update!(:registered_pads_offsets, &Map.delete(&1, pad_ref))

    {actions, timestamp_queue}
  end

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
    |> Map.update!(:blocking_registered_pads, &MapSet.delete(&1, pad_ref))
    |> Map.update!(:registered_pads_offsets, &Map.delete(&1, pad_ref))
    |> Map.update!(:awaiting_pads, &List.delete(&1, pad_ref))
  end

  defp push_item(%__MODULE__{closed?: true}, pad_ref, item) do
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

  defp push_item(%__MODULE__{} = timestamp_queue, pad_ref, item) do
    timestamp_queue
    |> maybe_push_pad_on_heap_on_new_item(pad_ref, item)
    |> Map.update!(:pad_queues, &Map.put_new(&1, pad_ref, new_pad_queue()))
    |> update_in([:pad_queues, pad_ref, :qex], &Qex.push(&1, item))
  end

  defp maybe_push_pad_on_heap_on_new_item(timestamp_queue, pad_ref, item) do
    pad_queue = Map.get(timestamp_queue.pad_queues, pad_ref)
    empty_qex = Qex.new()

    case {item, pad_queue} do
      {{:buffer, buffer}, nil} ->
        priority =
          case timestamp_queue.synchronization_strategy do
            :synchronize_on_arrival ->
              -timestamp_queue.current_queue_time

            :explicit_offsets ->
              offset = Map.get(timestamp_queue.registered_pads_offsets, pad_ref, 0)
              offset - (buffer.dts || buffer.pts)
          end

        push_pad_on_heap(timestamp_queue, pad_ref, priority)

      {{:buffer, buffer}, pad_queue} when pad_queue.qex == empty_qex ->
        push_pad_on_heap(timestamp_queue, pad_ref, -buffer_time(buffer, pad_queue))

      {_non_buffer, pad_queue} when pad_queue == nil or pad_queue.qex == empty_qex ->
        push_pad_on_heap(timestamp_queue, pad_ref, :infinity)

      _else ->
        timestamp_queue
    end
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
  Pushes a buffer and pops items from the queue while they are available.

  Buffers pushed to the queue must have a non-`nil` `dts` or `pts`.

  A buffer `b` from pad `p` is available, if all pads different than `p`
    - either have a buffer in the queue, that is older than `b`
    - or haven't ever had any buffer on the queue
    - or have end of stream pushed on the queue.

  An item other than a buffer is considered available if all newer buffers on the same pad are
  available.

  The returned value is a suggested actions list, a list of popped items and the updated queue.

  If the amount of buffers associated with any pad in the queue
   - falls below the `pause_demand_boundary`, the suggested actions list contains
     `t:Membrane.Action.resume_auto_demand()` actions
   - rises above the `pause_demand_boundary`, the suggested actions list contains
     `t:Membrane.Action.pause_auto_demand()` action.
  """
  @spec push_buffer_and_pop_available_items(t(), Pad.ref(), Buffer.t()) ::
          {[Action.pause_auto_demand() | Action.resume_auto_demand()], [popped_value()], t()}
  def push_buffer_and_pop_available_items(%__MODULE__{} = timestamp_queue, pad_ref, buffer) do
    push_buffer_and_pop(timestamp_queue, pad_ref, buffer, &pop_available_items/1)
  end

  @type chunk :: [popped_value()]

  @doc """
  Pops chunked items from the queue while there are enough available items, to create a chunk.

  If there are some available items in the queue, but there are not enough of them to create
  a chunk, it won't be created.

  A buffer `b` from pad `p` is available, if all pads different than `p`
    - either have a buffer in the queue, that is older than `b`
    - or haven't ever had any buffer on the queue
    - or have end of stream pushed on the queue.

  An item other than a buffer is considered available if all newer buffers on the same pad are
  available.

  The returned value is a suggested actions list, a list of chunks of popped items and the updated
  queue.

  If the amount of buffers associated with any pad in the queue falls below the
  `pause_demand_boundary`, the suggested actions list contains `t:Membrane.Action.resume_auto_demand()`
  actions, otherwise it is an empty list.
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
  Pushes a buffer and pops chunked items from the queue, while there are enough available items,
  to create a chunk.

  Buffers pushed to the queue must have a non-`nil` `dts` or `pts`.

  If there are some available items in the queue, but there are not enough of them to create
  a chunk, it won't be created.

  A buffer `b` from pad `p` is available, if all pads different than `p`
    - either have a buffer in the queue, that is older than `b`
    - or haven't ever had any buffer on the queue
    - or have end of stream pushed on the queue.

  An item other than a buffer is considered available if all newer buffers on the same pad are
  available.

  The returned value is a suggested actions list, a list of chunks of popped items and the updated
  queue.

  If the amount of buffers associated with any pad in the queue
   - falls below the `pause_demand_boundary`, the suggested actions list contains
     `t:Membrane.Action.resume_auto_demand()` actions
   - rises above the `pause_demand_boundary`, the suggested actions list contains
     `t:Membrane.Action.pause_auto_demand()` action.
  """
  @spec push_buffer_and_pop_chunked(t(), Pad.ref(), Buffer.t()) ::
          {[Action.pause_auto_demand() | Action.resume_auto_demand()], [popped_value()], t()}
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
        {_pad_queue, timestamp_queue} = pop_in(timestamp_queue, [:pad_queues, pad_ref])
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

  defp push_pad_on_heap(timestamp_queue, pad_ref, priority) do
    heap_item = {priority, pad_ref}
    Map.update!(timestamp_queue, :pads_heap, &Heap.push(&1, heap_item))
  end

  @doc """
  Pops all items in the proper order and closes the queue.

  After being closed, queue is unable handle any new buffer/stream format/event/end of stream.

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
end
