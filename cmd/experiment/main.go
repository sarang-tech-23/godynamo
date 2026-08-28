// Command experiment is a runnable tour of context.Context, built to
// explain the two lines in internal/transport/handlers.go:
//
//	ctx, cancel := context.WithTimeout(r.Context(), forwardTimeout)
//	defer cancel()
//
// Run it with:
//
//	go run ./cmd/experiment
//
// Nothing here is part of the key-value store; it exists purely to make
// the mechanism visible.
package main

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"
)

func main() {
	doneIsAChannel()
	cancelIsABroadcast()
	timeoutFiresItself()
	firstTriggerWins()
	cancellationFlowsDownTheTree()
	theEverydaySelectLoop()
	whatCancelPrevents()
	errTellsYouWhichTriggerFired()
	howForwardUsesAllOfThis()
}

// ---------------------------------------------------------------------
// 1. A context is, at bottom, a channel that gets closed.
// ---------------------------------------------------------------------

func doneIsAChannel() {
	section(1, "ctx.Done() is just a channel that gets closed")

	ctx, cancel := context.WithCancel(context.Background())

	done := ctx.Done()
	fmt.Printf("Done() returns a %T -- a receive-only channel of empty structs\n", done)
	fmt.Println("  before cancel():", state(done))

	cancel()
	fmt.Println("  after cancel(): ", state(done))

	// Receiving from a closed channel returns immediately, forever. That
	// is the whole trick: a close is a permanent, broadcast signal, unlike
	// a send, which exactly one receiver would consume.
	<-done
	<-done
	fmt.Println("  a closed channel can be received from any number of times")
}

func cancelIsABroadcast() {
	section(2, "one cancel() wakes every goroutine watching")

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ctx.Done() // all three park on the same channel
			fmt.Printf("  watcher %d woke up: %v\n", i, ctx.Err())
		}()
	}

	time.Sleep(50 * time.Millisecond)
	fmt.Println("  calling cancel() exactly once...")
	cancel()
	wg.Wait()
}

// ---------------------------------------------------------------------
// 2. Three things can close that channel.
// ---------------------------------------------------------------------

func timeoutFiresItself() {
	section(3, "WithTimeout closes Done() on its own, with no help from you")

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	deadline, ok := ctx.Deadline()
	fmt.Printf("  has a deadline: %v, about %v from now\n", ok, time.Until(deadline).Round(10*time.Millisecond))

	<-ctx.Done() // blocks until the internal timer fires
	fmt.Printf("  Done() closed after %v, Err() = %v\n",
		time.Since(start).Round(10*time.Millisecond), ctx.Err())
	fmt.Println("  nobody called cancel() here -- WithTimeout's own timer did it")
}

func firstTriggerWins() {
	section(4, "whichever trigger fires first wins; later ones are no-ops")

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel() // beats the 10s timer by a mile
	}()

	<-ctx.Done()
	fmt.Printf("  10s budget, but hand-cancelled after %v: %v\n",
		time.Since(start).Round(10*time.Millisecond), ctx.Err())

	// cancel is idempotent -- calling it repeatedly is safe and is why
	// `defer cancel()` alongside an explicit cancel() is never a bug.
	cancel()
	cancel()
	fmt.Println("  calling cancel() twice more changed nothing:", ctx.Err())
}

func cancellationFlowsDownTheTree() {
	section(5, "cancelling a parent cancels its entire subtree")

	parent, cancelParent := context.WithCancel(context.Background())
	child, cancelChild := context.WithCancel(parent)
	defer cancelChild()

	// A one-hour budget -- and yet it will die in a moment.
	grandchild, cancelGrandchild := context.WithTimeout(child, time.Hour)
	defer cancelGrandchild()

	fmt.Println("  before: parent =", state(parent.Done()), "| grandchild =", state(grandchild.Done()))
	cancelParent()
	time.Sleep(10 * time.Millisecond)
	fmt.Println("  after:  parent =", state(parent.Done()), "| grandchild =", state(grandchild.Done()))

	fmt.Println("  the grandchild had a 1h timeout and still died instantly")
	fmt.Println("  cancellation flows DOWN only: cancelling a child never touches its parent")
}

// ---------------------------------------------------------------------
// 3. How you actually consume it.
// ---------------------------------------------------------------------

func theEverydaySelectLoop() {
	section(6, "the everyday pattern: select on ctx.Done() beside real work")

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for unit := 1; ; unit++ {
			select {
			case <-ctx.Done():
				fmt.Printf("  worker stopped voluntarily after %d units: %v\n", unit-1, ctx.Err())
				return
			case <-time.After(80 * time.Millisecond): // stands in for real work
				fmt.Printf("  worker finished unit %d\n", unit)
			}
		}
	}()
	<-finished

	fmt.Println("  note: the worker CHOSE to return. A context cannot kill a")
	fmt.Println("  goroutine -- it only delivers a signal that well-written code checks.")
}

func whatCancelPrevents() {
	section(7, "what the `defer cancel()` habit actually prevents")

	before := runtime.NumGoroutine()

	// Leaky: this goroutine waits on a context nobody ever cancels, so it
	// parks on <-Done() for the lifetime of the process.
	leakCtx, leakCancel := context.WithCancel(context.Background())
	go func() {
		<-leakCtx.Done()
	}()

	// Correct: identical shape, but the context is cancelled, so the
	// goroutine actually finishes.
	goodCtx, goodCancel := context.WithCancel(context.Background())
	tidy := make(chan struct{})
	go func() {
		<-goodCtx.Done()
		close(tidy)
	}()
	goodCancel()
	<-tidy

	time.Sleep(50 * time.Millisecond)
	fmt.Printf("  goroutines at start: %d, now: %d\n", before, runtime.NumGoroutine())
	fmt.Println("  the cancelled one exited; the other is still parked on <-Done()")
	fmt.Println("  a WithTimeout you never cancel also keeps its timer armed and stays")
	fmt.Println("  registered on its parent -- on a long-lived parent, that is a real leak")

	leakCancel() // clean up, so this demo does not leak for real
	time.Sleep(50 * time.Millisecond)
	fmt.Printf("  after finally cancelling it: %d\n", runtime.NumGoroutine())
}

func errTellsYouWhichTriggerFired() {
	section(8, "ctx.Err() reports which of the three triggers fired")

	live, cancelLive := context.WithCancel(context.Background())
	defer cancelLive()
	fmt.Printf("  still running:  Err() = %v\n", live.Err())

	byHand, cancelByHand := context.WithCancel(context.Background())
	cancelByHand()
	fmt.Printf("  after cancel(): Err() = %-18v errors.Is(..., context.Canceled)         = %v\n",
		byHand.Err(), errors.Is(byHand.Err(), context.Canceled))

	byClock, cancelByClock := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelByClock()
	<-byClock.Done()
	fmt.Printf("  after timeout:  Err() = %-18v errors.Is(..., context.DeadlineExceeded) = %v\n",
		byClock.Err(), errors.Is(byClock.Err(), context.DeadlineExceeded))

	fmt.Println("  this distinction matters: a deadline means the peer was too slow (502),")
	fmt.Println("  a cancel usually means the caller left and nobody wants the answer.")
}

// ---------------------------------------------------------------------
// 4. Tying it back to forward().
// ---------------------------------------------------------------------

func howForwardUsesAllOfThis() {
	section(9, "how forward() in internal/transport uses every piece above")

	fmt.Println("  ctx, cancel := context.WithTimeout(r.Context(), forwardTimeout)")
	fmt.Println("  -> r.Context() is cancelled by net/http when the client disconnects")
	fmt.Println("  -> WithTimeout adds a second, independent trigger: the 2s budget")
	fmt.Print("  -> so the forwarded call dies on whichever happens first\n\n")

	// Case A: the peer answers comfortably inside the budget.
	inbound, clientLeft := context.WithCancel(context.Background())
	defer clientLeft()
	ctx, cancel := context.WithTimeout(inbound, 2*time.Second)
	defer cancel()
	fmt.Println("  peer answers in 100ms:      ", outcome(callPeer(ctx, 100*time.Millisecond)))

	// Case B: the client hangs up while the peer is still thinking.
	inbound2, clientLeft2 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithTimeout(inbound2, 2*time.Second)
	defer cancel2()
	go func() {
		time.Sleep(100 * time.Millisecond)
		clientLeft2() // the browser/CLI went away
	}()
	fmt.Println("  client hangs up at 100ms:   ", outcome(callPeer(ctx2, 5*time.Second)))

	// Case C: the peer is simply too slow and the budget runs out.
	ctx3, cancel3 := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel3()
	fmt.Println("  peer needs 5s, 300ms budget:", outcome(callPeer(ctx3, 5*time.Second)))
}

// callPeer stands in for s.client.Do(outReq). The real http.Client watches
// the request's context in exactly this shape: finish the work, or abandon
// it the moment the context is done -- whichever comes first.
func callPeer(ctx context.Context, work time.Duration) error {
	select {
	case <-time.After(work):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func outcome(err error) string {
	switch {
	case err == nil:
		return "response relayed back to the client"
	case errors.Is(err, context.DeadlineExceeded):
		return "gave up on the peer -> 502 to the client"
	case errors.Is(err, context.Canceled):
		return "abandoned early -- nobody is left to read the answer"
	default:
		return err.Error()
	}
}

// ---------------------------------------------------------------------

func section(n int, title string) {
	fmt.Printf("\n=== %d. %s ===\n", n, title)
}

// state reports whether a Done() channel has been closed yet, without
// blocking: the default case is what makes the check non-blocking.
func state(done <-chan struct{}) string {
	select {
	case <-done:
		return "closed (cancelled)"
	default:
		return "open (live)"
	}
}
