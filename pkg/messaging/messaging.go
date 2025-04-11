package messaging

// MessageSender defines an interface for sending messages
// This helps decouple the core package from specific implementations
// like Kafka in the queue package
type MessageSender interface {
	SendDoneMessage(done *DoneMessage) error
}

// DoneMessage represents the message structure for the Done object
// to be sent to Kafka.
type DoneMessage struct {
	OrderID      string
	ExecutedQty  string
	RemainingQty string
	Trades       []Trade
	Canceled     []string
	Activated    []string
	Stored       bool
	Quantity     string
	Processed    string
	Left         string
}

// Trade represents a single trade execution
type Trade struct {
	OrderID  string
	Role     string
	Price    string
	Quantity string
	IsQuote  bool
}
