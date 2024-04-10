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

  @type pad_queue :: %{
          timestamp_offset: integer(),
          qex: Qex.t(),
          buffers_size: non_neg_integer(),
          buffers_number: non_neg_integer(),
          paused_demand?: boolean(),
          end_of_stream?: boolean(),
          use_pts?: boolean() | nil,
          max_timestamp_on_qex: Membrane.Time.t() | nil
        }

  @typedoc """
  A queue, that accepts buffers, stream formats and events from various pads and sorts them based on
  their timestamps.
  """
  @opaque t :: %__MODULE__{
            current_queue_time: Membrane.Time.t(),
            pause_demand_boundary: pos_integer() | :infinity,
            pause_demand_boundary_unit: :buffers | :bytes,
            pad_queues: %{optional(Pad.ref()) => pad_queue()},
            pads_heap: Heap.t(),
            registered_pads: MapSet.t(),
            awaiting_pads: [Pad.ref()],
            closed?: boolean()
          }

  defstruct current_queue_time: Membrane.Time.seconds(0),
            pause_demand_boundary: :infinity,
            pause_demand_boundary_unit: :buffers,
            pad_queues: %{},
            pads_heap: Heap.max(),
            registered_pads: MapSet.new(),
            awaiting_pads: [],
            closed?: false

  @typedoc """
  Options passed to #{inspect(__MODULE__)}.new/1.

  Following options are allowed:
    - `:pause_demand_boundary` - positive integer or `:infinity` (default to `:infinity`). Tells, what
      amount of buffers associated with specific pad must be stored in the queue, to pause auto demand.
    - `:pause_demand_boundary_unit` - `:buffers` or `:bytes` (deafult to `:buffers`). Tells, in which metric
      `:pause_demand_boundary` is specified.
  """
  @type options :: [
          pause_demand_boundary: pos_integer() | :infinity,
          pause_demand_boundary_unit: :buffers | :bytes
        ]

  @spec new(options) :: t()
  def new(options \\ []) do
    [pause_demand_boundary: boundary, pause_demand_boundary_unit: unit] =
      options
      |> Keyword.validate!(
        pause_demand_boundary: :infinity,
        pause_demand_boundary_unit: :buffers
      )
      |> Enum.sort()

    %__MODULE__{
      pause_demand_boundary: boundary,
      pause_demand_boundary_unit: unit
    }
  end

  @doc """
  Registers an input pad in the queue without pushing anything on that pad.

  Once a pad is registered, the `pop_batch/3` function won't return buffers
  until a `buffer` or `end_of_stream` is available on the registered pad.

  Pushing a buffer on an unregistered pad automatically registers it.
  """
  @spec register_pad(t(), Pad.ref()) :: t()
  def register_pad(%__MODULE__{} = timestamp_queue, pad_ref) do
    timestamp_queue
    |> Map.update!(:registered_pads, &MapSet.put(&1, pad_ref))
  end

  @doc """
  Pushes a buffer associated with a specified pad to the queue.

  Returns a suggested actions list and the updated queue.

  If amount of buffers associated with specified pad in the queue just exceded
  `pause_demand_boundary`, the suggested actions list contains `t:Action.pause_auto_demand()`
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
    timestamp_queue
    |> push_item(pad_ref, {:buffer, buffer})
    |> Map.update!(:registered_pads, &MapSet.delete(&1, pad_ref))
    |> Map.update!(:awaiting_pads, &List.delete(&1, pad_ref))
    |> get_and_update_in([:pad_queues, pad_ref], fn pad_queue ->
      pad_queue
      |> Map.merge(%{
        buffers_size: pad_queue.buffers_size + buffer_size(timestamp_queue, buffer),
        buffers_number: pad_queue.buffers_number + 1
      })
      |> Map.update!(:timestamp_offset, fn
        nil -> timestamp_queue.current_queue_time - (buffer.dts || buffer.pts)
        valid_offset -> valid_offset
      end)
      |> Map.update!(:use_pts?, fn
        nil -> buffer.dts == nil
        valid_boolean -> valid_boolean
      end)
      |> check_timestamps_consistency!(buffer, pad_ref)
      |> actions_after_pushing_buffer(pad_ref, timestamp_queue.pause_demand_boundary)
    end)
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
    |> Map.update!(:registered_pads, &MapSet.delete(&1, pad_ref))
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
      {{:buffer, _buffer}, nil} ->
        push_pad_on_heap(timestamp_queue, pad_ref, -timestamp_queue.current_queue_time)

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
      paused_demand?: false,
      end_of_stream?: false,
      use_pts?: nil,
      max_timestamp_on_qex: nil,
      recently_returned_timestamp: nil
    }
  end

  defp actions_after_pushing_buffer(pad_queue, pad_ref, pause_demand_boundary) do
    if not pad_queue.paused_demand? and pad_queue.buffers_size >= pause_demand_boundary do
      {[pause_auto_demand: pad_ref], %{pad_queue | paused_demand?: true}}
    else
      {[], pad_queue}
    end
  end

  defp buffer_size(%__MODULE__{pause_demand_boundary_unit: :buffers}, _buffer),
    do: 1

  defp buffer_size(%__MODULE__{pause_demand_boundary_unit: :bytes}, %Buffer{payload: payload}),
    do: byte_size(payload)

  defp buffer_time(%Buffer{dts: dts}, %{use_pts?: false, timestamp_offset: timestamp_offset}),
    do: dts + timestamp_offset

  defp buffer_time(%Buffer{pts: pts}, %{use_pts?: true, timestamp_offset: timestamp_offset}),
    do: pts + timestamp_offset

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

  The returned value is a suggested actions list, a list of popped buffers and the updated queue.

  If the amount of buffers associated with any pad in the queue falls below the
  `pause_demand_boundary`, the suggested actions list contains `t:Action.resume_auto_demand()`
  actions, otherwise it is an empty list.
  """
  @spec pop_batch(t()) :: {[Action.resume_auto_demand()], [popped_value()], t()}
  def pop_batch(%__MODULE__{} = timestamp_queue) do
    do_pop_batch(timestamp_queue, [], [])
  end

  defp do_pop_batch(%__MODULE__{} = timestamp_queue, actions_acc, items_acc) do
    try_return_buffer? =
      MapSet.size(timestamp_queue.registered_pads) == 0 and timestamp_queue.awaiting_pads == []

    case Heap.root(timestamp_queue.pads_heap) do
      {priority, pad_ref} when try_return_buffer? or priority == :infinity ->
        {actions, items, timestamp_queue} =
          timestamp_queue
          |> Map.update!(:pads_heap, &Heap.pop/1)
          |> pop_buffer_and_following_items(pad_ref)

        do_pop_batch(timestamp_queue, actions ++ actions_acc, items ++ items_acc)

      _other ->
        {actions_acc, Enum.reverse(items_acc), timestamp_queue}
    end
  end

  @spec pop_buffer_and_following_items(t(), Pad.ref()) ::
          {[Action.resume_auto_demand()], [popped_value()], t()}
  defp pop_buffer_and_following_items(%__MODULE__{} = timestamp_queue, pad_ref) do
    pad_queue = timestamp_queue.pad_queues |> Map.get(pad_ref)

    with {{:value, {:buffer, buffer}}, qex} <- Qex.pop(pad_queue.qex) do
      old_buffers_size = pad_queue.buffers_size

      pad_queue = %{
        pad_queue
        | qex: qex,
          buffers_size: old_buffers_size - buffer_size(timestamp_queue, buffer),
          buffers_number: pad_queue.buffers_number - 1
      }

      timestamp_queue =
        with %{buffers_number: 0, end_of_stream?: false} <- pad_queue do
          Map.update!(timestamp_queue, :awaiting_pads, &[pad_ref | &1])
        else
          _pad_queue -> timestamp_queue
        end

      timestamp_queue =
        %{timestamp_queue | current_queue_time: buffer_time(buffer, pad_queue)}
        |> put_in([:pad_queues, pad_ref], pad_queue)

      boundary = timestamp_queue.pause_demand_boundary

      actions =
        if pad_queue.buffers_size < boundary and old_buffers_size >= boundary,
          do: [resume_auto_demand: pad_ref],
          else: []

      items = [{pad_ref, {:buffer, buffer}}]

      pop_following_items(timestamp_queue, pad_ref, actions, items)
    else
      _other -> pop_following_items(timestamp_queue, pad_ref, [], [])
    end
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

  defp push_pad_on_heap(timestamp_queue, pad_ref, priority) do
    heap_item = {priority, pad_ref}
    Map.update!(timestamp_queue, :pads_heap, &Heap.push(&1, heap_item))
  end

  @spec flush_and_close(t()) :: {[Action.resume_auto_demand()], [popped_value()], t()}
  def flush_and_close(%__MODULE__{} = timestamp_queue) do
    %{timestamp_queue | closed?: true, registered_pads: MapSet.new(), awaiting_pads: []}
    |> Map.update!(
      :pad_queues,
      &Map.new(&1, fn {pad_ref, data} ->
        {pad_ref, %{data | end_of_stream?: true}}
      end)
    )
    |> pop_batch()
  end
end
