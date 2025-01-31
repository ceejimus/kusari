package utils

// this creates a rx/tx channel pair
// if the outbound channel is at capacity when new messages are received
// older messages will be thrown away to make room
//
// e.g. tx, rx := NewDroppingChannel(1024)
func NewDroppingChannel[T any](size int) (chan<- T, <-chan T) {
	in := make(chan T)        // caller's tx
	out := make(chan T, size) // caller's rx

	go func() {
		// simple select loop to toss old messages
		for msg := range in {
			select {
			case out <- msg:
			default:
				<-out
				out <- msg
			}
		}
		close(out)
	}()

	return in, out
}
