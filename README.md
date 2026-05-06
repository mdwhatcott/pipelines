# github.com/mdw-go/pipelines

A flexible yet minimal framework for setting up concurrent assembly lines in Go.

The assembly line is several groups of 'stations' chained together with channels. Each group consists of one or more 'stations', each reading off of the same input channel that the previous station group sends values to (and eventually closes). When a group consists of multiple stations, the [fan-out/fan-in algorithm described on the Go blog](https://go.dev/blog/pipelines) applies.

Stations are typed: each station declares the type of value it consumes and the type of value it produces. Channels between groups are `chan In` / `chan Out`, never `chan any`, so type errors are caught at compile time.

## Station interfaces

```go
type Station[In, Out any] interface {
    Do(input In, output func(Out))
}

type Finalizer[Out any] interface {
    Finalize(output func(Out))
}
```

- `In` and `Out` are the element types of the input and output channels.
- The `output` func sends a value to the next station group's input channel.
- Stations that also implement `Finalizer[Out]` have one last shot at sending values before their output channel is closed.
- Multiple stations in a single group result in a fan-out/fan-in.
- Values emitted by the last station in the pipeline have no downstream consumer and are logged via the configured `Logger`.

## Assembling a pipeline

```go
input := make(chan int)
go func() {
    defer close(input)
    for x := range 50 {
        input <- x
    }
}()

p0 := pipelines.New[int](input)
p1 := pipelines.Then(p0, []pipelines.Station[int, int]{NewSquares()})
p2 := pipelines.Then(p1, []pipelines.Station[int, int]{NewEvens()})
p3 := pipelines.Then(p2, []pipelines.Station[int, int]{NewFirstN(10)})
p4 := pipelines.Then(p3, []pipelines.Station[int, int64]{ // fan-out
    NewSum(), NewSum(), NewSum(), NewSum(), NewSum(),
})
p5 := pipelines.Then(p4, []pipelines.Station[int64, struct{}]{NewCatchAll()})
p5.Listen()
```

`Then` is a free generic function rather than a method because Go does not support generic methods that introduce type parameters beyond the receiver's. `Then` accepts a slice of stations (one for a single-station group, more for fan-out/fan-in) and any number of group options.

## Group options

```go
pipelines.BufferedOutput[Out](capacity)        // buffer the group's output channel
pipelines.SendViaSelect[Out](callback)         // skip values when the next channel is full, sending them to callback
```

See the test cases for fully-worked pipelines.
