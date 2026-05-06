package pipelines

import "sync"

func New[T any](input <-chan T, opts ...option) Pipeline[T] {
	c := new(config)
	c.apply(opts...)
	return Pipeline[T]{
		build:  func() <-chan T { return input },
		logger: c.logger,
	}
}

func Then[In, Out any](
	prev Pipeline[In],
	stations []Station[In, Out],
	opts ...groupOption[Out],
) Pipeline[Out] {
	if len(stations) == 0 {
		panic("pipelines: Then requires at least one station")
	}
	g := newGroup(stations, opts...)
	return Pipeline[Out]{
		build: func() <-chan Out {
			in := prev.build()
			out := make(chan Out, g.bufferCapacity)
			go g.run(in, out)
			return out
		},
		logger: prev.logger,
	}
}

type Pipeline[Out any] struct {
	build  func() <-chan Out
	logger Logger
}

func (this Pipeline[Out]) Listen() {
	for v := range this.build() {
		this.logger.Printf("value at end of pipeline: %v", v)
	}
}

type group[In, Out any] struct {
	groupConfig[Out]
	stations []Station[In, Out]
}

func (this *group[In, Out]) run(input <-chan In, output chan Out) {
	if len(this.stations) > 1 {
		this.runFannedOutStation(input, output)
	} else {
		this.runStation(this.stations[0], input, output)
	}
}
func (this *group[In, Out]) runFannedOutStation(input <-chan In, final chan Out) {
	defer close(final)
	var outs []chan Out
	for _, station := range this.stations {
		out := make(chan Out)
		outs = append(outs, out)
		go this.runStation(station, input, out)
	}
	var waiter sync.WaitGroup
	defer waiter.Wait()
	for _, out := range outs {
		waiter.Go(func() {
			for item := range out {
				final <- item
			}
		})
	}
}
func (this *group[In, Out]) runStation(station Station[In, Out], input <-chan In, output chan Out) {
	defer close(output)
	var out func(Out)
	if this.sendViaSelectCallback != nil {
		out = sendViaSelect(output, this.sendViaSelectCallback)
	} else {
		out = blockingSend(output)
	}
	if finalizer, ok := station.(Finalizer[Out]); ok {
		defer finalizer.Finalize(out)
	}
	for value := range input {
		station.Do(value, out)
	}
}

func sendViaSelect[Out any](output chan Out, callback func(Out)) func(Out) {
	return func(v Out) {
		select {
		case output <- v:
		default:
			callback(v)
		}
	}
}
func blockingSend[Out any](output chan Out) func(Out) {
	return func(v Out) { output <- v }
}
