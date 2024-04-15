defmodule Membrane.TimestampQueue.UnitTest do
  use ExUnit.Case, async: true

  require Membrane.Pad, as: Pad

  alias Membrane.Buffer
  alias Membrane.TimestampQueue

  defmodule StreamFormat do
    defstruct [:dts]
  end

  defmodule Event do
    defstruct [:dts]
  end

  test "queue raises on buffer with nil dts and pts" do
    assert_raise(RuntimeError, fn ->
      TimestampQueue.new()
      |> TimestampQueue.push_buffer(:input, %Buffer{payload: <<>>})
    end)
  end

  test "queue sorts some buffers from different pads based on buffer dts" do
    input_order = [9, 4, 7, 3, 1, 8, 5, 6, 2, 10]

    pad_generator = fn i -> Pad.ref(:input, i) end
    buffer_generator = fn i -> %Buffer{dts: i, payload: <<>>} end

    queue =
      input_order
      |> Enum.reduce(TimestampQueue.new(), fn i, queue ->
        assert {[], queue} =
                 TimestampQueue.push_buffer(queue, pad_generator.(i), %Buffer{
                   dts: 0,
                   payload: <<>>
                 })

        queue
      end)

    queue =
      input_order
      |> Enum.reduce(queue, fn i, queue ->
        assert {[], queue} =
                 TimestampQueue.push_buffer(queue, pad_generator.(i), buffer_generator.(i))

        queue
      end)

    # assert that queue won't pop last buffer from pad queue, if it hasn't recevied EoS on this pad
    assert {[], batch, queue} = TimestampQueue.pop_available_items(queue)
    batch_length = length(batch)

    batch
    |> Enum.zip(1..batch_length)
    |> Enum.each(fn
      {item, ^batch_length} -> assert {pad_generator.(1), {:buffer, buffer_generator.(1)}} == item
      {item, _idx} -> assert {_pad_ref, {:buffer, %Buffer{dts: 0}}} = item
    end)

    queue =
      input_order
      |> Enum.reduce(queue, fn i, queue ->
        TimestampQueue.push_end_of_stream(queue, pad_generator.(i))
      end)

    assert {[], batch, queue} = TimestampQueue.pop_available_items(queue)

    # assert batch
    expected_batch =
      input_order
      |> Enum.sort()
      |> Enum.flat_map(fn i ->
        if i == 1 do
          [{pad_generator.(i), :end_of_stream}]
        else
          [
            {pad_generator.(i), {:buffer, buffer_generator.(i)}},
            {pad_generator.(i), :end_of_stream}
          ]
        end
      end)

    assert batch == expected_batch

    # assert queue empty
    assert queue.pad_queues == TimestampQueue.new().pad_queues
    assert queue.pads_heap == TimestampQueue.new().pads_heap
  end

  test "queue sorts buffers a lot of buffers from different pads based on buffer dts" do
    pads_number = 100
    pad_items_number = 200

    dts_offsets =
      Map.new(1..pads_number, fn pad_idx ->
        {Pad.ref(:input, pad_idx), Enum.random(1..10_000)}
      end)

    pads_items =
      Map.new(1..pads_number, fn pad_idx ->
        pad_ref = Pad.ref(:input, pad_idx)
        dts_offset = dts_offsets[pad_ref]

        {items, _last_buffer_dts} =
          Enum.map_reduce(dts_offset..(dts_offset + pad_items_number - 1), dts_offset, fn idx,
                                                                                          last_buffer_dts ->
            if idx == dts_offset do
              {{:push_buffer, %Buffer{dts: idx, payload: <<>>}}, idx}
            else
              Enum.random([
                {{:push_buffer, %Buffer{dts: idx, payload: <<>>}}, idx},
                {{:push_event, %Event{dts: last_buffer_dts}}, last_buffer_dts},
                {{:push_stream_format, %StreamFormat{dts: last_buffer_dts}}, last_buffer_dts}
              ])
            end
          end)

        {pad_ref, items}
      end)

    queue = TimestampQueue.new()

    {pads_items, queue} =
      1..(pads_number * pad_items_number)
      |> Enum.reduce({pads_items, queue}, fn _i, {pads_items, queue} ->
        {pad_ref, items} = Enum.random(pads_items)
        [{fun_name, item} | items] = items

        pads_items =
          case items do
            [] -> Map.delete(pads_items, pad_ref)
            items -> Map.put(pads_items, pad_ref, items)
          end

        queue =
          case apply(TimestampQueue, fun_name, [queue, pad_ref, item]) do
            # if buffer
            {[], queue} -> queue
            # if event or stream_format
            queue -> queue
          end

        {pads_items, queue}
      end)

    queue =
      Enum.reduce(1..pads_number, queue, fn i, queue ->
        TimestampQueue.push_end_of_stream(queue, Pad.ref(:input, i))
      end)

    # sanity check, that test is written correctly
    assert %{} = pads_items

    assert {[], batch, _queue} = TimestampQueue.pop_available_items(queue)
    assert length(batch) == pads_number * pad_items_number + pads_number

    batch_without_eos = Enum.reject(batch, &match?({_pad_ref, :end_of_stream}, &1))

    sorted_batch_without_eos =
      batch_without_eos
      |> Enum.sort_by(fn {pad_ref, {_type, item}} -> item.dts - dts_offsets[pad_ref] end)

    assert batch_without_eos == sorted_batch_without_eos
  end

  test "queue prioritizes stream formats and buffers not preceded by a buffer" do
    queue = TimestampQueue.new()

    {[], queue} = TimestampQueue.push_buffer(queue, :a, %Buffer{dts: 1, payload: <<>>})
    {[], queue} = TimestampQueue.push_buffer(queue, :a, %Buffer{dts: 2, payload: <<>>})

    expected_batch = for i <- [1, 2], do: {:a, {:buffer, %Buffer{dts: i, payload: <<>>}}}
    assert {[], ^expected_batch, queue} = TimestampQueue.pop_available_items(queue)

    {[], queue} = TimestampQueue.push_buffer(queue, :a, %Buffer{dts: 3, payload: <<>>})
    queue = TimestampQueue.push_end_of_stream(queue, :a)

    queue = TimestampQueue.push_stream_format(queue, :b, %StreamFormat{})
    queue = TimestampQueue.push_event(queue, :b, %Event{})

    assert {[], batch, queue} = TimestampQueue.pop_available_items(queue)

    assert batch == [
             b: {:stream_format, %StreamFormat{}},
             b: {:event, %Event{}},
             a: {:buffer, %Buffer{dts: 3, payload: <<>>}},
             a: :end_of_stream
           ]

    assert {[], [], ^queue} = TimestampQueue.pop_available_items(queue)
  end

  [
    %{unit: :buffers, buffer_size: 1, buffer: %Buffer{dts: 0, payload: <<>>}},
    %{unit: :bytes, buffer_size: 100, buffer: %Buffer{dts: 0, payload: <<1::8*100>>}}
  ]
  |> Enum.map(fn params ->
    test "queue returns proper suggested actions when boundary unit is #{inspect(params.unit)}" do
      %{unit: unit, buffer_size: buffer_size, buffer: buffer} =
        unquote(Macro.escape(params))

      boundary_in_buff_no = 100
      boundary = buffer_size * boundary_in_buff_no

      queue =
        TimestampQueue.new(
          pause_demand_boundary: boundary,
          pause_demand_boundary_unit: unit
        )

      Enum.reduce(1..10, queue, fn _iteration, queue ->
        queue =
          1..(boundary_in_buff_no - 1)
          |> Enum.reduce(queue, fn _i, queue ->
            assert {[], queue} = TimestampQueue.push_buffer(queue, :input, buffer)
            queue
          end)

        assert {[pause_auto_demand: :input], queue} =
                 TimestampQueue.push_buffer(queue, :input, buffer)

        queue =
          1..(boundary_in_buff_no - 1)
          |> Enum.reduce(queue, fn _i, queue ->
            assert {[], queue} = TimestampQueue.push_buffer(queue, :input, buffer)
            queue
          end)

        pop_item = {:input, {:buffer, buffer}}

        expected_batch = for _i <- 1..(2 * boundary_in_buff_no - 1), do: pop_item

        assert {[resume_auto_demand: :input], ^expected_batch, queue} =
                 TimestampQueue.pop_available_items(queue)

        queue
      end)
    end
  end)

  test "queue returns proper suggested actions when boundary unit is :time" do
    queue =
      TimestampQueue.new(
        pause_demand_boundary: 100,
        pause_demand_boundary_unit: :time
      )

    Enum.reduce(1..10, queue, fn iteration, queue ->
      pts_offset = iteration * 100_000

      queue_below_boundary =
        Enum.concat(1..50, 50..100//10)
        |> Enum.reduce(queue, fn i, queue ->
          buffer = %Buffer{pts: pts_offset + i, payload: ""}
          assert {[], queue} = TimestampQueue.push_buffer(queue, :input, buffer)
          queue
        end)

      assert {[pause_auto_demand: :input], queue_above_boundary} =
               TimestampQueue.push_buffer(queue_below_boundary, :input, %Buffer{
                 pts: pts_offset + 101,
                 payload: ""
               })

      assert {[resume_auto_demand: :input], _batch, _queue} =
               TimestampQueue.pop_available_items(queue_above_boundary)

      assert {[pause_auto_demand: :input], queue_above_boundary} =
               TimestampQueue.push_buffer(queue_below_boundary, :input, %Buffer{
                 pts: pts_offset + 1000,
                 payload: ""
               })

      assert {[resume_auto_demand: :input], _batch, queue} =
               TimestampQueue.pop_available_items(queue_above_boundary)

      queue
    end)
  end

  test "queue sorts buffers from various pads when they aren't linked in the same moment" do
    iteration_size = 100
    iterations = 100

    1..iterations
    |> Enum.reduce(TimestampQueue.new(), fn pads_in_iteration, queue ->
      pads = for i <- 1..pads_in_iteration, do: Pad.ref(:input, i)

      new_pad = Pad.ref(:input, pads_in_iteration)
      new_pad_timestamp_field = if div(pads_in_iteration, 2) == 1, do: :dts, else: :pts

      buffer =
        %Buffer{payload: <<>>}
        |> Map.put(new_pad_timestamp_field, 0)

      {[], queue} = TimestampQueue.push_buffer(queue, new_pad, buffer)

      queue =
        pads
        |> Enum.reduce(queue, fn pad_ref, queue ->
          Pad.ref(:input, pad_idx) = pad_ref
          pad_offset = iteration_size * (pads_in_iteration - pad_idx)
          timestamp_field = if div(pad_idx, 2) == 1, do: :dts, else: :pts

          (pad_offset + 1)..(pad_offset + iteration_size)
          |> Enum.reduce(queue, fn timestamp, queue ->
            buffer =
              %Buffer{payload: <<>>}
              |> Map.put(timestamp_field, timestamp)

            {[], queue} = TimestampQueue.push_buffer(queue, pad_ref, buffer)
            queue
          end)
        end)

      {[], batch, queue} = TimestampQueue.pop_available_items(queue)

      sorted_batch =
        batch
        |> Enum.sort_by(fn {Pad.ref(:input, pad_idx), {:buffer, buffer}} ->
          (buffer.dts || buffer.pts) + pad_idx * iteration_size
        end)

      assert batch == sorted_batch

      Enum.group_by(batch, &elem(&1, 0))
      |> Map.new(fn {pad, items} -> {pad, length(items)} end)

      expected_batch_length =
        if pads_in_iteration == 1,
          do: iteration_size + 1,
          else: pads_in_iteration * iteration_size

      assert length(batch) == expected_batch_length

      queue
    end)
  end

  test "queue doesn't return any buffer, if it should wait on buffer from registered pad" do
    queue =
      TimestampQueue.new()
      |> TimestampQueue.register_pad(:a)
      |> TimestampQueue.register_pad(:b)

    events = for i <- 1..1000, do: %Event{dts: i}
    buffers = for i <- 1..1000, do: %Buffer{dts: i, payload: <<>>}

    queue =
      events
      |> Enum.reduce(queue, fn event, queue ->
        queue
        |> TimestampQueue.push_event(:a, event)
        |> TimestampQueue.push_event(:b, event)
      end)

    queue =
      buffers
      |> Enum.reduce(queue, fn buffer, queue ->
        {[], queue} = TimestampQueue.push_buffer(queue, :a, buffer)
        queue
      end)

    {[], batch, queue} = TimestampQueue.pop_available_items(queue)

    grouped_batch = Enum.group_by(batch, &elem(&1, 0), &(elem(&1, 1) |> elem(1)))
    assert grouped_batch == %{a: events, b: events}

    assert {[], [], queue} = TimestampQueue.pop_available_items(queue)

    queue =
      buffers
      |> Enum.reduce(queue, fn buffer, queue ->
        {[], queue} = TimestampQueue.push_buffer(queue, :b, buffer)
        queue
      end)

    {[], batch, _queue} = TimestampQueue.pop_available_items(queue)

    sorted_batch = Enum.sort_by(batch, fn {_pad_ref, {:buffer, buffer}} -> buffer.dts end)
    assert batch == sorted_batch

    grouped_batch = Enum.group_by(batch, &elem(&1, 0), &(elem(&1, 1) |> elem(1)))

    assert grouped_batch |> Map.keys() |> MapSet.new() == MapSet.new([:a, :b])

    assert grouped_batch |> Map.values() |> MapSet.new() ==
             MapSet.new([buffers, List.delete_at(buffers, 999)])
  end

  test "queue returns events and stream formats, even if it cannot return next buffer" do
    queue = TimestampQueue.new()

    {[], queue} = TimestampQueue.push_buffer(queue, :a, %Buffer{dts: 0, payload: ""})
    {[], queue} = TimestampQueue.push_buffer(queue, :b, %Buffer{dts: 0, payload: ""})

    {[], batch, queue} = TimestampQueue.pop_available_items(queue)
    assert [{pad_ref, {:buffer, _buffer}}] = batch

    queue =
      Enum.reduce(1..10, queue, fn _i, queue ->
        TimestampQueue.push_event(queue, pad_ref, %Event{})
      end)

    {[], batch, queue} = TimestampQueue.pop_available_items(queue)

    assert batch == for(_i <- 1..10, do: {pad_ref, {:event, %Event{}}})

    [opposite_pad_ref] = List.delete([:a, :b], pad_ref)

    buffers =
      [{pad_ref, %Buffer{dts: 10, payload: ""}}] ++
        Enum.map(1..9, &{opposite_pad_ref, %Buffer{dts: &1, payload: ""}})

    queue =
      Enum.reduce(buffers, queue, fn {pad, buffer}, queue ->
        {[], queue} = TimestampQueue.push_buffer(queue, pad, buffer)
        queue
      end)
      |> TimestampQueue.push_end_of_stream(:a)
      |> TimestampQueue.push_end_of_stream(:b)

    {[], batch, _queue} = TimestampQueue.pop_available_items(queue)

    expected_batch =
      Enum.map(0..9, &{opposite_pad_ref, {:buffer, %Buffer{dts: &1, payload: ""}}})
      |> Enum.concat([
        {opposite_pad_ref, :end_of_stream},
        {pad_ref, {:buffer, %Buffer{dts: 10, payload: ""}}},
        {pad_ref, :end_of_stream}
      ])

    assert batch == expected_batch
  end

  test "flush_and_close/1 works like pop_available_items/1 on a queue with end of stream on every pad" do
    push_functions = [
      fn queue, pad_ref, _i -> TimestampQueue.push_event(queue, pad_ref, %Event{}) end,
      fn queue, pad_ref, _i ->
        TimestampQueue.push_stream_format(queue, pad_ref, %StreamFormat{})
      end,
      fn queue, pad_ref, i ->
        {[], queue} = TimestampQueue.push_buffer(queue, pad_ref, %Buffer{dts: i, payload: ""})
        queue
      end
    ]

    for _repetition <- 1..10 do
      full_queue =
        Enum.reduce(1..10_000, TimestampQueue.new(), fn i, queue ->
          pad_ref = Pad.ref(:input, Enum.random(1..100))
          Enum.random(push_functions) |> apply([queue, pad_ref, i])
        end)

      {[], flush_batch, closed_queue} = TimestampQueue.flush_and_close(full_queue)

      {[], pop_available_items, popped_queue} =
        Enum.reduce(1..100, full_queue, fn i, queue ->
          TimestampQueue.push_end_of_stream(queue, Pad.ref(:input, i))
        end)
        |> TimestampQueue.pop_available_items()

      expected_flush_batch =
        pop_available_items
        |> Enum.reject(&match?({_pad_ref, :end_of_stream}, &1))

      assert flush_batch == expected_flush_batch

      assert closed_queue.pads_heap == popped_queue.pads_heap
      assert closed_queue.pad_queues == popped_queue.pad_queues

      assert_raise RuntimeError, ~r/Unable to push .* already closed/, fn ->
        buffer = %Buffer{dts: 10_001, payload: ""}
        TimestampQueue.push_buffer(closed_queue, Pad.ref(:input, 0), buffer)
      end
    end
  end

  test "pop_chunked/1 returns properly chunked buffers from a single pad" do
    overbound = Membrane.Time.seconds(10)
    step = Membrane.Time.millisecond()
    chunk_duration = Membrane.Time.second()

    queue = TimestampQueue.new(chunk_duration: chunk_duration)

    buffers =
      1..overbound//step
      |> Enum.map(&%Buffer{pts: &1, payload: ""})

    {[], queue} =
      buffers
      |> Enum.reduce(queue, fn buffer, queue ->
        {[], queue} = TimestampQueue.push_buffer(queue, :input, buffer)
        queue
      end)
      |> TimestampQueue.push_buffer(:input, %Buffer{
        pts: overbound + Membrane.Time.nanosecond(),
        payload: ""
      })

    {[], given_chunks, _queue} = TimestampQueue.pop_chunked(queue)

    expected_chunks =
      buffers
      |> Enum.group_by(&((&1.pts / chunk_duration) |> trunc()))
      |> Enum.sort()
      |> Enum.map(fn {_sec, chunk} ->
        Enum.map(chunk, &{:input, {:buffer, &1}})
      end)

    assert given_chunks == expected_chunks
  end
end
