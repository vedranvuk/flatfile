package flatfile

import "time"

type reader struct {
	activereaders int
	closechan     chan bool
}

func newReader() *reader {
	r := &reader{
		closechan: make(chan bool),
	}
	go r.listener()
	return r
}

// timeoutf
func timeoutf(renew chan bool, timeout time.Duration) {
	for {
		select {
		case <-time.After(timeout):
		}
	}
	renew <- true
}

// listener
func (r *reader) listener() error {
	for {
		select {
		case <-r.closechan:
		}
	}
}

// Close
func (r *reader) Close() error {
	r.closechan <- true
	return nil
}
