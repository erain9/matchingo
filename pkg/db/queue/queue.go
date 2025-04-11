package queue

import (
	"fmt"

	"github.com/IBM/sarama"
	orderbookpb "github.com/erain9/matchingo/pkg/api/proto"
	"github.com/erain9/matchingo/pkg/messaging"
	"google.golang.org/protobuf/proto"
)

var (
	brokerList      = "localhost:9092"
	topic           = "test-msg-queue"
	newSyncProducer = sarama.NewSyncProducer
)

// QueueMessageSender implements the MessageSender interface
// for sending messages to Kafka
type QueueMessageSender struct{}

// SendDoneMessage sends the DoneMessage to the Kafka queue
func (q *QueueMessageSender) SendDoneMessage(done *messaging.DoneMessage) error {
	// Convert to proto message
	protoMsg := &orderbookpb.DoneMessage{
		OrderId:           done.OrderID,
		ExecutedQuantity:  done.ExecutedQty,
		RemainingQuantity: done.RemainingQty,
		Canceled:          done.Canceled,
		Activated:         done.Activated,
		Stored:            done.Stored,
		Quantity:          done.Quantity,
		Processed:         done.Processed,
		Left:              done.Left,
	}

	// Convert trades to proto format
	if len(done.Trades) > 0 {
		protoMsg.Trades = make([]*orderbookpb.Trade, 0, len(done.Trades))
		for _, trade := range done.Trades {
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
		return fmt.Errorf("failed to marshal done message: %v", err)
	}

	// Create a Kafka producer message
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(messageBytes),
	}

	// Send the message to Kafka
	producer, err := newSyncProducer([]string{brokerList}, nil)
	if err != nil {
		return fmt.Errorf("failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	_, _, err = producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message to Kafka: %v", err)
	}

	return nil
}

// QueueMessageConsumer implements the MessageConsumer interface
// for consuming messages from Kafka
type QueueMessageConsumer struct {
	consumer sarama.Consumer
	done     chan struct{}
}

// NewQueueMessageConsumer creates a new Kafka consumer
func NewQueueMessageConsumer() (*QueueMessageConsumer, error) {
	consumer, err := sarama.NewConsumer([]string{brokerList}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %v", err)
	}

	return &QueueMessageConsumer{
		consumer: consumer,
		done:     make(chan struct{}),
	}, nil
}

// Close closes the Kafka consumer
func (q *QueueMessageConsumer) Close() error {
	close(q.done)
	return q.consumer.Close()
}

// ConsumeDoneMessages starts consuming DoneMessages from Kafka
func (q *QueueMessageConsumer) ConsumeDoneMessages(handler func(*messaging.DoneMessage) error) error {
	partitionConsumer, err := q.consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		return fmt.Errorf("failed to start consumer for partition: %v", err)
	}
	defer partitionConsumer.Close()

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			// Deserialize the protobuf message
			protoMsg := &orderbookpb.DoneMessage{}
			if err := proto.Unmarshal(msg.Value, protoMsg); err != nil {
				fmt.Printf("Failed to unmarshal message: %v\n", err)
				continue
			}

			// Convert to DoneMessage
			doneMsg := &messaging.DoneMessage{
				OrderID:      protoMsg.OrderId,
				ExecutedQty:  protoMsg.ExecutedQuantity,
				RemainingQty: protoMsg.RemainingQuantity,
				Canceled:     protoMsg.Canceled,
				Activated:    protoMsg.Activated,
				Stored:       protoMsg.Stored,
				Quantity:     protoMsg.Quantity,
				Processed:    protoMsg.Processed,
				Left:         protoMsg.Left,
			}

			// Convert trades
			if len(protoMsg.Trades) > 0 {
				doneMsg.Trades = make([]messaging.Trade, 0, len(protoMsg.Trades))
				for _, trade := range protoMsg.Trades {
					doneMsg.Trades = append(doneMsg.Trades, messaging.Trade{
						OrderID:  trade.OrderId,
						Role:     trade.Role,
						Price:    trade.Price,
						Quantity: trade.Quantity,
						IsQuote:  trade.IsQuote,
					})
				}
			}

			// Process the message
			if err := handler(doneMsg); err != nil {
				fmt.Printf("Failed to process message: %v\n", err)
			}

		case <-q.done:
			return nil
		}
	}
}
