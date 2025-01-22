package utils

func NewDroppingChannel[T any](size int) (chan<- T, <-chan T) {
	in := make(chan T)
	out := make(chan T, size)

	go func() {
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
