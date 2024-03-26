# Membrane Timestamp Queue

[![Hex.pm](https://img.shields.io/hexpm/v/membrane_timestamp_queue.svg)](https://hex.pm/packages/membrane_timestamp_queue)
[![API Docs](https://img.shields.io/badge/api-docs-yellow.svg?style=flat)](https://hexdocs.pm/membrane_timestamp_queue)
[![CircleCI](https://circleci.com/gh/membraneframework/membrane_timestamp_queue.svg?style=svg)](https://circleci.com/gh/membraneframework/membrane_timestamp_queue)

This repository contains implementation of `Membrane.TimestampQueue`, a helper queue that is aimed to help manage flow control in `Membrane` elements with pads with `flow_control: :auto`.

It's a part of the [Membrane Framework](https://membrane.stream).

## Installation

The package can be installed by adding `membrane_timestamp_queue` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:membrane_timestamp_queue, "~> 0.1.0"}
  ]
end
```

## Copyright and License

Copyright 2020, [Software Mansion](https://swmansion.com/?utm_source=git&utm_medium=readme&utm_campaign=membrane_timestamp_queue)

[![Software Mansion](https://logo.swmansion.com/logo?color=white&variant=desktop&width=200&tag=membrane-github)](https://swmansion.com/?utm_source=git&utm_medium=readme&utm_campaign=membrane_timestamp_queue)

Licensed under the [Apache License, Version 2.0](LICENSE)
