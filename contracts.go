package pipelines

type Logger interface {
	Printf(format string, args ...any)
}

type Station[In, Out any] interface {
	Do(input In, output func(Out))
}

type Finalizer[Out any] interface {
	Finalize(output func(Out))
}
