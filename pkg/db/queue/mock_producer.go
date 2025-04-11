package queue

import (
	"github.com/IBM/sarama"
)

// mockProducer implements just enough of sarama.SyncProducer for our tests
type mockProducer struct {
	sentMessages []*sarama.ProducerMessage
}

func (m *mockProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	m.sentMessages = append(m.sentMessages, msg)
	return 0, 0, nil
}

func (m *mockProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	m.sentMessages = append(m.sentMessages, msgs...)
	return nil
}

func (m *mockProducer) Close() error {
	return nil
}

func (m *mockProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	return 0
}

func (m *mockProducer) BeginTxn() error {
	return nil
}

func (m *mockProducer) CommitTxn() error {
	return nil
}

func (m *mockProducer) AbortTxn() error {
	return nil
}

func (m *mockProducer) AddMessageToTxn(msg *sarama.ConsumerMessage, groupID string, metadata *string) error {
	return nil
}

func (m *mockProducer) AddOffsetsToTxn(offsets map[string][]*sarama.PartitionOffsetMetadata, groupID string) error {
	return nil
}

func (m *mockProducer) IsTransactional() bool {
	return false
}
