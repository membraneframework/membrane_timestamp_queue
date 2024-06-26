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
        assert {_actions, queue} =
                 TimestampQueue.push_buffer(queue, pad_generator.(i), %Buffer{
                   dts: 0,
                   payload: <<>>
                 })

        queue
      end)

    queue =
      input_order
      |> Enum.reduce(queue, fn i, queue ->
        assert {_actions, queue} =
                 TimestampQueue.push_buffer(queue, pad_generator.(i), buffer_generator.(i))

        queue
      end)

    # assert that queue won't pop last buffer from pad queue, if it hasn't recevied EoS on this pad
    assert {_actions, batch, queue} = TimestampQueue.pop_available_items(queue)
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

    assert {_actions, batch, queue} = TimestampQueue.pop_available_items(queue)

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
            {_actions, queue} -> queue
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

    assert {_actions, batch, _queue} = TimestampQueue.pop_available_items(queue)
    assert length(batch) == pads_number * pad_items_number + pads_number

    batch_without_eos = Enum.reject(batch, &match?({_pad_ref, :end_of_stream}, &1))

    sorted_batch_without_eos =
      batch_without_eos
      |> Enum.sort_by(fn {pad_ref, {_type, item}} -> item.dts - dts_offsets[pad_ref] end)

    assert batch_without_eos == sorted_batch_without_eos
  end

  test "queue prioritizes stream formats and buffers not preceded by a buffer" do
    queue = TimestampQueue.new()

    {_actions, queue} = TimestampQueue.push_buffer(queue, :a, %Buffer{dts: 1, payload: <<>>})
    {_actions, queue} = TimestampQueue.push_buffer(queue, :a, %Buffer{dts: 2, payload: <<>>})

    expected_batch = for i <- [1, 2], do: {:a, {:buffer, %Buffer{dts: i, payload: <<>>}}}
    assert {_actions, ^expected_batch, queue} = TimestampQueue.pop_available_items(queue)

    {_actions, queue} = TimestampQueue.push_buffer(queue, :a, %Buffer{dts: 3, payload: <<>>})
    queue = TimestampQueue.push_end_of_stream(queue, :a)

    queue = TimestampQueue.push_stream_format(queue, :b, %StreamFormat{})
    queue = TimestampQueue.push_event(queue, :b, %Event{})

    assert {_actions, batch, queue} = TimestampQueue.pop_available_items(queue)

    assert batch == [
             b: {:stream_format, %StreamFormat{}},
             b: {:event, %Event{}},
             a: {:buffer, %Buffer{dts: 3, payload: <<>>}},
             a: :end_of_stream
           ]

    assert {_actions, [], ^queue} = TimestampQueue.pop_available_items(queue)
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
        TimestampQueue.new(pause_demand_boundary: {unit, boundary})

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
    queue = TimestampQueue.new(pause_demand_boundary: {:time, 100})

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

      {_actions, queue} = TimestampQueue.push_buffer(queue, new_pad, buffer)

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

            {_actions, queue} = TimestampQueue.push_buffer(queue, pad_ref, buffer)
            queue
          end)
        end)

      {_actions, batch, queue} = TimestampQueue.pop_available_items(queue)

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

  test "queue doesn't return any buffer, if it should wait on buffer from the registered pad" do
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
        {_actions, queue} = TimestampQueue.push_buffer(queue, :a, buffer)
        queue
      end)

    {_actions, batch, queue} = TimestampQueue.pop_available_items(queue)

    grouped_batch = Enum.group_by(batch, &elem(&1, 0), &(elem(&1, 1) |> elem(1)))
    assert grouped_batch == %{a: events, b: events}

    assert {_actions, [], queue} = TimestampQueue.pop_available_items(queue)

    queue =
      buffers
      |> Enum.reduce(queue, fn buffer, queue ->
        {_actions, queue} = TimestampQueue.push_buffer(queue, :b, buffer)
        queue
      end)

    {_actions, batch, _queue} = TimestampQueue.pop_available_items(queue)

    sorted_batch = Enum.sort_by(batch, fn {_pad_ref, {:buffer, buffer}} -> buffer.dts end)
    assert batch == sorted_batch

    grouped_batch = Enum.group_by(batch, &elem(&1, 0), &(elem(&1, 1) |> elem(1)))

    assert grouped_batch |> Map.keys() |> Enum.sort() == [:a, :b]

    assert grouped_batch |> Map.values() |> MapSet.new() ==
             MapSet.new([buffers, List.delete_at(buffers, 999)])
  end

  test "queue doesn't wait on buffers from pad registered with option wait_on_buffers?: false" do
    buffer = %Buffer{dts: 0, payload: ""}

    assert {_actions, [a: {:buffer, ^buffer}], _queue} =
             TimestampQueue.new()
             |> TimestampQueue.register_pad(:b, wait_on_buffers?: false)
             |> TimestampQueue.push_buffer_and_pop_available_items(:a, buffer)
  end

  test "queue correctly sorts items from various pads when synchronization stratey is :explicit_offsets" do
    offsets = %{a: 0, b: 10_001, c: 20_002, d: 30_003}

    queue =
      TimestampQueue.new(synchronization_strategy: :explicit_offsets)
      |> TimestampQueue.register_pad(:b, timestamp_offset: offsets.b, wait_on_buffers?: false)
      |> TimestampQueue.register_pad(:c, timestamp_offset: offsets.c, wait_on_buffers?: false)
      |> TimestampQueue.register_pad(:d, timestamp_offset: offsets.d, wait_on_buffers?: false)

    {data, queue} =
      1..100_000//10
      |> Enum.map_reduce(queue, fn i, queue ->
        pad_ref = Enum.random([:a, :b, :c, :d])
        buffer = %Buffer{pts: i, payload: ""}

        {_actions, queue} = TimestampQueue.push_buffer(queue, pad_ref, buffer)

        {{pad_ref, {:buffer, buffer}}, queue}
      end)

    expected_batch =
      Enum.sort_by(data, fn {pad_ref, {:buffer, buffer}} ->
        buffer.pts - offsets[pad_ref]
      end)

    {_actions, given_batch, _queue} = TimestampQueue.flush_and_close(queue)

    assert given_batch == expected_batch
  end

  test "queue returns events and stream formats, even if it cannot return next buffer" do
    queue = TimestampQueue.new()

    {_actions, queue} = TimestampQueue.push_buffer(queue, :a, %Buffer{dts: 0, payload: ""})
    {_actions, queue} = TimestampQueue.push_buffer(queue, :b, %Buffer{dts: 0, payload: ""})

    {_actions, batch, queue} = TimestampQueue.pop_available_items(queue)
    assert [{pad_ref, {:buffer, _buffer}}] = batch

    queue =
      Enum.reduce(1..10, queue, fn _i, queue ->
        TimestampQueue.push_event(queue, pad_ref, %Event{})
      end)

    {_actions, batch, queue} = TimestampQueue.pop_available_items(queue)

    assert batch == for(_i <- 1..10, do: {pad_ref, {:event, %Event{}}})

    [opposite_pad_ref] = List.delete([:a, :b], pad_ref)

    buffers =
      [{pad_ref, %Buffer{dts: 10, payload: ""}}] ++
        Enum.map(1..9, &{opposite_pad_ref, %Buffer{dts: &1, payload: ""}})

    queue =
      Enum.reduce(buffers, queue, fn {pad, buffer}, queue ->
        {_actions, queue} = TimestampQueue.push_buffer(queue, pad, buffer)
        queue
      end)
      |> TimestampQueue.push_end_of_stream(:a)
      |> TimestampQueue.push_end_of_stream(:b)

    {_actions, batch, _queue} = TimestampQueue.pop_available_items(queue)

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
        {_actions, queue} =
          TimestampQueue.push_buffer(queue, pad_ref, %Buffer{dts: i, payload: ""})

        queue
      end
    ]

    for _repetition <- 1..10 do
      full_queue =
        Enum.reduce(1..10_000, TimestampQueue.new(), fn i, queue ->
          pad_ref = Pad.ref(:input, Enum.random(1..100))
          Enum.random(push_functions) |> apply([queue, pad_ref, i])
        end)

      {_actions, flush_batch, closed_queue} = TimestampQueue.flush_and_close(full_queue)

      {_actions, pop_available_items, popped_queue} =
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

      assert closed_queue.closed?

      assert_raise RuntimeError, ~r/Unable to push .* already closed/, fn ->
        buffer = %Buffer{dts: 10_001, payload: ""}
        TimestampQueue.push_buffer(closed_queue, Pad.ref(:input, 0), buffer)
      end
    end
  end

  test "pop_chunked/1 returns properly chunked buffers from a single pad" do
    upperbound = Membrane.Time.seconds(10)
    step = Membrane.Time.millisecond()
    chunk_duration = Membrane.Time.second()

    queue = TimestampQueue.new(chunk_duration: chunk_duration)

    buffers =
      1..upperbound//step
      |> Enum.map(&%Buffer{pts: &1, payload: ""})

    {_actions, queue} =
      buffers
      |> Enum.reduce(queue, fn buffer, queue ->
        {_actions, queue} = TimestampQueue.push_buffer(queue, :input, buffer)
        queue
      end)
      |> TimestampQueue.push_buffer(:input, %Buffer{
        pts: upperbound + Membrane.Time.nanosecond(),
        payload: ""
      })

    {_actions, given_chunks, _queue} = TimestampQueue.pop_chunked(queue)

    expected_chunks =
      buffers
      |> Enum.group_by(&((&1.pts / chunk_duration) |> trunc()))
      |> Enum.sort()
      |> Enum.map(fn {_sec, chunk} ->
        Enum.map(chunk, &{:input, {:buffer, &1}})
      end)

    assert given_chunks == expected_chunks
  end

  test "pop_chunked/1 returns properly chunked buffers from many pads" do
    queue = TimestampQueue.new(chunk_duration: Membrane.Time.second())

    zero_buffer = %Buffer{dts: 0, payload: ""}

    queue =
      1..100
      |> Enum.reduce(queue, fn i, queue ->
        pad_ref = Pad.ref(:input, i)
        {_actions, queue} = TimestampQueue.push_buffer(queue, pad_ref, zero_buffer)

        {_actions, queue} =
          TimestampQueue.push_buffer(queue, pad_ref, %Buffer{
            dts: Membrane.Time.seconds(i),
            payload: ""
          })

        queue
      end)
      |> TimestampQueue.push_end_of_stream(Pad.ref(:input, 1))

    {_actions, first_batch, queue} = TimestampQueue.pop_chunked(queue)

    assert [first_chunk, second_chunk] = first_batch

    sorted_expected_first_chunk =
      Enum.map(1..100, &{Pad.ref(:input, &1), {:buffer, zero_buffer}})
      |> Enum.sort()

    assert Enum.sort(first_chunk) == sorted_expected_first_chunk

    expected_second_chunk = [
      {Pad.ref(:input, 1), {:buffer, %Buffer{dts: Membrane.Time.second(), payload: ""}}},
      {Pad.ref(:input, 1), :end_of_stream}
    ]

    assert second_chunk == expected_second_chunk

    queue =
      2..100
      |> Enum.reduce(queue, &TimestampQueue.push_end_of_stream(&2, Pad.ref(:input, &1)))

    {_actions, second_batch, _queue} = TimestampQueue.pop_chunked(queue)

    expected_second_batch =
      Enum.map(2..100, fn i ->
        pad_ref = Pad.ref(:input, i)
        buffer = %Buffer{dts: Membrane.Time.seconds(i), payload: ""}

        [{pad_ref, {:buffer, buffer}}, {pad_ref, :end_of_stream}]
      end)

    assert second_batch == expected_second_batch
  end

  test "push_buffer_and_pop_* functions work as composition of push and pop functions" do
    queue =
      TimestampQueue.new(
        pause_demand_boundary: {:buffers, 100},
        chunk_duration: Membrane.Time.milliseconds(10)
      )

    buffer = %Buffer{dts: Membrane.Time.seconds(0), payload: ""}
    {[], queue} = TimestampQueue.push_buffer(queue, :b, buffer)
    {[], [b: {:buffer, ^buffer}], queue} = TimestampQueue.pop_available_items(queue)

    buffer = %Buffer{dts: Membrane.Time.second(), payload: ""}
    {[], queue} = TimestampQueue.push_buffer(queue, :a, buffer)

    queue =
      Enum.reduce(1..98, queue, fn i, queue ->
        dts = Membrane.Time.second() + Membrane.Time.milliseconds(i)
        buffer = %Buffer{dts: dts, payload: ""}

        {[], queue} = TimestampQueue.push_buffer(queue, :a, buffer)
        {[], queue} = TimestampQueue.push_buffer(queue, :b, buffer)

        queue
      end)

    dts = Membrane.Time.second() + Membrane.Time.milliseconds(99)
    buffer = %Buffer{dts: dts, payload: ""}

    {[pause_auto_demand: :a], queue} = TimestampQueue.push_buffer(queue, :a, buffer)
    {[], queue} = TimestampQueue.push_buffer(queue, :b, buffer)

    [
      {&TimestampQueue.pop_available_items/1,
       &TimestampQueue.push_buffer_and_pop_available_items/3},
      {&TimestampQueue.pop_chunked/1, &TimestampQueue.push_buffer_and_pop_chunked/3}
    ]
    |> Enum.each(fn {pop_fun, push_and_pop_fun} ->
      buffer = %Buffer{dts: Membrane.Time.second() + Membrane.Time.milliseconds(99), payload: ""}

      {given_actions, given_items, _queue} = push_and_pop_fun.(queue, :b, buffer)

      {push_actions, queue} = TimestampQueue.push_buffer(queue, :b, buffer)
      {pop_actions, expected_items, _queue} = pop_fun.(queue)

      assert given_actions == push_actions ++ pop_actions
      assert given_items == expected_items
    end)
  end

  test "push_buffer_and_pop_* functions don't return pause and resume demand actions for the same pad" do
    queue =
      TimestampQueue.new(
        pause_demand_boundary: {:buffers, 100},
        chunk_duration: Membrane.Time.milliseconds(10)
      )

    queue =
      Enum.reduce(1..99, queue, fn i, queue ->
        buffer = %Buffer{dts: Membrane.Time.milliseconds(i), payload: ""}
        {[], queue} = TimestampQueue.push_buffer(queue, :input, buffer)
        queue
      end)

    [
      {&TimestampQueue.pop_available_items/1,
       &TimestampQueue.push_buffer_and_pop_available_items/3},
      {&TimestampQueue.pop_chunked/1, &TimestampQueue.push_buffer_and_pop_chunked/3}
    ]
    |> Enum.each(fn {pop_fun, push_and_pop_fun} ->
      buffer = %Buffer{dts: Membrane.Time.milliseconds(100), payload: ""}

      assert {[], items, _queue} = push_and_pop_fun.(queue, :input, buffer)

      {_actions, queue} = TimestampQueue.push_buffer(queue, :input, buffer)
      {_actions, expected_items, _queue} = pop_fun.(queue)

      assert items == expected_items
    end)
  end

  test "has_pad?/2 and pads/1 work correctly" do
    queue = TimestampQueue.new()

    queue =
      Enum.reduce([:b, :f], queue, fn pad_ref, queue ->
        TimestampQueue.register_pad(queue, pad_ref)
      end)

    queue =
      Enum.reduce([:a, :b], queue, fn pad_ref, queue ->
        TimestampQueue.push_event(queue, pad_ref, %Event{})
      end)

    queue =
      Enum.reduce([:c, :d, :h], queue, fn pad_ref, queue ->
        buffer = %Buffer{dts: 0, payload: ""}
        {_actions, queue} = TimestampQueue.push_buffer(queue, pad_ref, buffer)
        queue
      end)

    queue =
      Enum.reduce([:c, :h], queue, fn pad_ref, queue ->
        buffer = %Buffer{dts: 10, payload: ""}
        {_actions, queue} = TimestampQueue.push_buffer(queue, pad_ref, buffer)
        queue
      end)

    queue =
      Enum.reduce([:b, :d, :f, :g], queue, fn pad_ref, queue ->
        TimestampQueue.push_end_of_stream(queue, pad_ref)
      end)

    {_actions, _items, queue} = TimestampQueue.pop_available_items(queue)

    queue = TimestampQueue.register_pad(queue, :e)

    pads_in_queue = [:a, :c, :e, :h]
    pads_beyond_queue = [:b, :d, :f, :g]

    assert TimestampQueue.pads(queue) == MapSet.new(pads_in_queue)

    for pad_ref <- pads_in_queue do
      assert TimestampQueue.has_pad?(queue, pad_ref)
    end

    for pad_ref <- pads_beyond_queue do
      assert not TimestampQueue.has_pad?(queue, pad_ref)
    end
  end
end
