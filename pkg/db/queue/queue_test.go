package queue

import (
	"testing"
	"time"

	"github.com/IBM/sarama"
	orderbookpb "github.com/erain9/matchingo/pkg/api/proto"
	"github.com/erain9/matchingo/pkg/messaging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type mockConsumer struct {
	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
}

func (m *mockConsumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	return &mockPartitionConsumer{
		messages: m.messages,
		errors:   m.errors,
	}, nil
}

func (m *mockConsumer) Topics() ([]string, error) {
	return []string{}, nil
}

func (m *mockConsumer) Partitions(topic string) ([]int32, error) {
	return []int32{}, nil
}

func (m *mockConsumer) HighWaterMarks() map[string]map[int32]int64 {
	return nil
}

func (m *mockConsumer) Close() error {
	close(m.messages)
	close(m.errors)
	return nil
}

func (m *mockConsumer) Pause(topicPartitions map[string][]int32) {}

func (m *mockConsumer) Resume(topicPartitions map[string][]int32) {}

func (m *mockConsumer) PauseAll() {}

func (m *mockConsumer) ResumeAll() {}

type mockPartitionConsumer struct {
	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
}

func (m *mockPartitionConsumer) AsyncClose() {}

func (m *mockPartitionConsumer) Close() error {
	return nil
}

func (m *mockPartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return m.messages
}

func (m *mockPartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return m.errors
}

func (m *mockPartitionConsumer) HighWaterMarkOffset() int64 {
	return 0
}

func (m *mockPartitionConsumer) IsPaused() bool {
	return false
}

func (m *mockPartitionConsumer) Pause() {}

func (m *mockPartitionConsumer) Resume() {}

func TestQueueMessageSender_SendDoneMessage(t *testing.T) {
	// Create a test message
	doneMessage := &messaging.DoneMessage{
		OrderID:      "test-order-1",
		ExecutedQty:  "100.5",
		RemainingQty: "50.0",
		Trades: []messaging.Trade{
			{
				OrderID:  "trade-1",
				Role:     "MAKER",
				Price:    "100.0",
				Quantity: "50.0",
				IsQuote:  true,
			},
		},
		Canceled:  []string{"cancel-1", "cancel-2"},
		Activated: []string{"activate-1"},
		Stored:    true,
		Quantity:  "150.5",
		Processed: "100.5",
		Left:      "50.0",
	}

	// Create sender with mock producer
	sender := &QueueMessageSender{}

	// Override the producer creation with our mock
	oldNewSyncProducer := newSyncProducer
	defer func() { newSyncProducer = oldNewSyncProducer }()

	mockProd := &mockProducer{}
	newSyncProducer = func(addrs []string, config *sarama.Config) (sarama.SyncProducer, error) {
		return mockProd, nil
	}

	// Test sending message
	err := sender.SendDoneMessage(doneMessage)
	require.NoError(t, err)

	// Verify the message was sent
	require.Len(t, mockProd.sentMessages, 1)
	msg := mockProd.sentMessages[0]

	// Verify the message content
	require.Equal(t, topic, msg.Topic)

	// Unmarshal the message value to verify its content
	var protoMsg orderbookpb.DoneMessage
	err = proto.Unmarshal(msg.Value.(sarama.ByteEncoder), &protoMsg)
	require.NoError(t, err)

	require.Equal(t, doneMessage.OrderID, protoMsg.OrderId)
	require.Equal(t, doneMessage.ExecutedQty, protoMsg.ExecutedQuantity)
	require.Equal(t, doneMessage.RemainingQty, protoMsg.RemainingQuantity)
	require.Equal(t, doneMessage.Quantity, protoMsg.Quantity)
	require.Equal(t, doneMessage.Processed, protoMsg.Processed)
	require.Equal(t, doneMessage.Left, protoMsg.Left)
	require.Equal(t, doneMessage.Stored, protoMsg.Stored)
	require.Equal(t, len(doneMessage.Trades), len(protoMsg.Trades))
	require.Equal(t, len(doneMessage.Canceled), len(protoMsg.Canceled))
	require.Equal(t, len(doneMessage.Activated), len(protoMsg.Activated))
}

func TestQueueMessageConsumer_ConsumeDoneMessages(t *testing.T) {
	// Create a test message
	expectedMessage := &messaging.DoneMessage{
		OrderID:      "test-order-1",
		ExecutedQty:  "100.5",
		RemainingQty: "50.0",
		Trades: []messaging.Trade{
			{
				OrderID:  "trade-1",
				Role:     "MAKER",
				Price:    "100.0",
				Quantity: "50.0",
				IsQuote:  true,
			},
		},
		Canceled:  []string{"cancel-1", "cancel-2"},
		Activated: []string{"activate-1"},
		Stored:    true,
		Quantity:  "150.5",
		Processed: "100.5",
		Left:      "50.0",
	}

	// Create a mock consumer
	mockConsumer := &mockConsumer{
		messages: make(chan *sarama.ConsumerMessage, 1),
		errors:   make(chan *sarama.ConsumerError, 1),
	}

	// Create consumer
	consumer := &QueueMessageConsumer{
		consumer: mockConsumer,
		done:     make(chan struct{}),
	}

	// Create a channel to receive the processed message
	receivedMessage := make(chan *messaging.DoneMessage, 1)

	// Start consuming in a goroutine
	go func() {
		err := consumer.ConsumeDoneMessages(func(msg *messaging.DoneMessage) error {
			receivedMessage <- msg
			return nil
		})
		require.NoError(t, err)
	}()

	// Send a test message
	mockConsumer.messages <- &sarama.ConsumerMessage{
		Value: mustMarshalProto(t, expectedMessage),
	}

	// Wait for message to be processed
	select {
	case msg := <-receivedMessage:
		assert.Equal(t, expectedMessage.OrderID, msg.OrderID)
		assert.Equal(t, expectedMessage.ExecutedQty, msg.ExecutedQty)
		assert.Equal(t, expectedMessage.RemainingQty, msg.RemainingQty)
		assert.Equal(t, expectedMessage.Quantity, msg.Quantity)
		assert.Equal(t, expectedMessage.Processed, msg.Processed)
		assert.Equal(t, expectedMessage.Left, msg.Left)
		assert.Equal(t, expectedMessage.Stored, msg.Stored)
		assert.Equal(t, len(expectedMessage.Trades), len(msg.Trades))
		assert.Equal(t, len(expectedMessage.Canceled), len(msg.Canceled))
		assert.Equal(t, len(expectedMessage.Activated), len(msg.Activated))
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}

	// Close the consumer
	err := consumer.Close()
	require.NoError(t, err)
}

func mustMarshalProto(t *testing.T, msg *messaging.DoneMessage) []byte {
	// Convert to proto message
	protoMsg := &orderbookpb.DoneMessage{
		OrderId:           msg.OrderID,
		ExecutedQuantity:  msg.ExecutedQty,
		RemainingQuantity: msg.RemainingQty,
		Canceled:          msg.Canceled,
		Activated:         msg.Activated,
		Stored:            msg.Stored,
		Quantity:          msg.Quantity,
		Processed:         msg.Processed,
		Left:              msg.Left,
	}

	// Convert trades to proto format
	if len(msg.Trades) > 0 {
		protoMsg.Trades = make([]*orderbookpb.Trade, 0, len(msg.Trades))
		for _, trade := range msg.Trades {
			protoMsg.Trades = append(protoMsg.Trades, &orderbookpb.Trade{
				OrderId:  trade.OrderID,
				Role:     trade.Role,
				Price:    trade.Price,
				Quantity: trade.Quantity,
				IsQuote:  trade.IsQuote,
			})
		}
	}

	// Serialize to protobuf
	messageBytes, err := proto.Marshal(protoMsg)
	if err != nil {
		t.Fatalf("failed to marshal done message: %v", err)
	}

	return messageBytes
}
