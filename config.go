package pipelines

type config struct {
	logger Logger
}

func (this *config) apply(options ...option) {
	for _, option := range append(Options.defaults(), options...) {
		if option != nil {
			option(this)
		}
	}
}

type option func(*config)

var Options options

type options struct{}

func (options) Logger(logger Logger) option {
	return func(c *config) { c.logger = logger }
}

func (options) defaults(options ...option) []option {
	return append([]option{
		Options.Logger(nop{}),
	}, options...)
}

type nop struct{}

func (nop) Printf(_ string, _ ...any) {}

type groupConfig[Out any] struct {
	bufferCapacity        int
	sendViaSelectCallback func(Out)
}

type groupOption[Out any] func(*groupConfig[Out])

// BufferedOutput ensures that the associated group's stations emit to a buffered channel.
// The provided capacity will be set to 1 if a lower value is provided.
// A capacity of 1 is equivalent to an unbuffered channel.
func BufferedOutput[Out any](capacity int) groupOption[Out] {
	return func(c *groupConfig[Out]) {
		c.bufferCapacity = max(unbufferedChannelCapacity, capacity)
	}
}

// SendViaSelect (with a non-nil callback) employs a channel send operation as part of a select statement. The default
// case passes items to the provided callback when the input channel to the next station is full.
// See https://go.dev/ref/spec#Select_statements for technical details.
// When provided a nil callback (the default) a traditional channel send operation is used, which will block when the
// input channel to the next station is full.
// (WARNING: May cause the entire pipeline to hang/deadlock in the case of a station with an errant infinite loop!)
func SendViaSelect[Out any](callback func(Out)) groupOption[Out] {
	return func(c *groupConfig[Out]) { c.sendViaSelectCallback = callback }
}

func newGroup[In, Out any](stations []Station[In, Out], opts ...groupOption[Out]) *group[In, Out] {
	cfg := groupConfig[Out]{bufferCapacity: unbufferedChannelCapacity}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return &group[In, Out]{
		groupConfig: cfg,
		stations:    stations,
	}
}

const unbufferedChannelCapacity = 1
