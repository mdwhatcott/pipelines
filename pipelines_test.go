package pipelines_test

import (
	"reflect"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/mdw-go/pipelines/v3"
)

func TestNoStations_AllValuesLogged(t *testing.T) {
	input := make(chan int)
	go func() {
		defer close(input)
		for x := range 10 {
			input <- x
		}
	}()
	logger := &TLogger{T: t}
	pipeline := pipelines.New[int](input,
		pipelines.Options.Logger(logger),
	)
	pipeline.Listen()

	if logger.count != 10 {
		t.Errorf("got %d log calls, should have 10", logger.count)
	}
}

// Test a somewhat interesting pipeline example, based on this Clojure threading macro example:
// https://clojuredocs.org/clojure.core/-%3E%3E#example-542692c8c026201cdc326a52
// (->> (range) (map #(* % %)) (filter even?) (take 10) (reduce +))  ; output: 1140
// Coincidentally, using github.com/mdw-go/funcy/ranger you can achieve the same result as follows:
// Reduce(op.Add, 0, Take(10, Filter(is.Even, Map(op.Square, RangeOpen(0, 1)))))
func TestPipelineExample(t *testing.T) {
	input := make(chan int)
	go func() {
		defer close(input)
		for x := range 50 {
			input <- x
		}
	}()

	sum := new(atomic.Int64)
	closed := new(atomic.Int64)
	var (
		squares = NewSquares()
		evens   = NewEvens()
		firstN  = NewFirstN(10)
		sums    = []pipelines.Station[int, int64]{
			NewSum(sum, closed),
			NewSum(sum, closed),
			NewSum(sum, closed),
			NewSum(sum, closed),
			NewSum(sum, closed),
		}
		catchAll = NewCatchAll()
	)
	p0 := pipelines.New[int](input,
		pipelines.Options.Logger(&TLogger{T: t}),
	)
	p1 := pipelines.Then(p0, []pipelines.Station[int, int]{squares})
	p2 := pipelines.Then(p1, []pipelines.Station[int, int]{evens})
	p3 := pipelines.Then(p2, []pipelines.Station[int, int]{firstN})
	p4 := pipelines.Then(p3, sums) // fan-out
	p5 := pipelines.Then(p4, []pipelines.Station[int64, struct{}]{catchAll})
	p5.Listen()

	const expected = 1140
	if total := sum.Load(); total != expected {
		t.Errorf("Expected %d, got %d", expected, total)
	}
	if final := int(closed.Load()); final != len(sums) {
		t.Errorf("Expected %d, got %d", len(sums), final)
	}
	sort.Ints(catchAll.final)
	if !reflect.DeepEqual(catchAll.final, []int{1, 2, 3, 4, 5}) {
		t.Errorf("Expected %d, got %d", []int{1, 2, 3, 4, 5}, catchAll.final)
	}
}

type TLogger struct {
	*testing.T
	count int
}

func (this *TLogger) Printf(format string, args ...any) {
	this.Helper()
	this.Logf(format, args...)
	this.count++
}

///////////////////////////////

type Squares struct{}

func NewSquares() *Squares {
	return &Squares{}
}

func (this *Squares) Do(input int, output func(int)) {
	output(input * input)
}

///////////////////////////////

type Evens struct{}

func NewEvens() *Evens {
	return &Evens{}
}

func (this *Evens) Do(input int, output func(int)) {
	if input%2 == 0 {
		output(input)
	}
}

///////////////////////////////

type FirstN struct {
	N       *atomic.Int64
	handled *atomic.Int64
}

func NewFirstN(n int64) *FirstN {
	N := new(atomic.Int64)
	N.Add(n)
	return &FirstN{N: N, handled: new(atomic.Int64)}
}

func (this *FirstN) Do(input int, output func(int)) {
	if this.handled.Load() >= this.N.Load() {
		return
	}
	output(input)
	this.handled.Add(1)
}

///////////////////////////////

// Sum aggregates ints arriving via Do and emits a finalization marker (a
// monotonically increasing count, one per Sum station) via Finalize. Do does
// not emit; the sum is read by the test from the shared *atomic.Int64.
type Sum struct {
	sum       *atomic.Int64
	finalized *atomic.Int64
}

func NewSum(sum, finalized *atomic.Int64) *Sum {
	return &Sum{sum: sum, finalized: finalized}
}

func (this *Sum) Do(input int, _ func(int64)) {
	this.sum.Add(int64(input))
}

func (this *Sum) Finalize(output func(int64)) {
	output(this.finalized.Add(1))
}

///////////////////////////////

type CatchAll struct {
	final []int
}

func NewCatchAll() *CatchAll {
	return &CatchAll{}
}

func (this *CatchAll) Do(input int64, _ func(struct{})) {
	this.final = append(this.final, int(input))
}
