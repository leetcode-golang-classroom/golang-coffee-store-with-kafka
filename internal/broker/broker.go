package broker

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/IBM/sarama"
)

type Broker struct {
	uri            string
	publisher      sarama.SyncProducer
	publisher_lock sync.RWMutex
	consumer       sarama.Consumer
	consumer_lock  sync.RWMutex
}

func NewBroker(uri string) (*Broker, error) {
	producer_config := sarama.NewConfig()
	producer_config.Producer.RequiredAcks = sarama.WaitForAll
	producer_config.Producer.Partitioner = sarama.NewRandomPartitioner
	producer_config.Producer.Return.Successes = true
	producer_config.Producer.Retry.Max = 5
	publisher, err := sarama.NewSyncProducer([]string{uri}, producer_config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect kafka producer: %w", err)
	}
	consumer_config := sarama.NewConfig()
	consumer_config.Consumer.Return.Errors = true
	consumer, err := sarama.NewConsumer([]string{uri}, consumer_config)
	if err != nil {
		defer publisher.Close()
		return nil, fmt.Errorf("failed to connect kafka consumer: %w", err)
	}
	return &Broker{
		uri:       uri,
		publisher: publisher,
		consumer:  consumer,
	}, nil
}
func (broker *Broker) PushOrderToQueue(ctx context.Context, topic string, message []byte) error {
	broker.publisher_lock.Lock()
	defer broker.publisher_lock.Unlock()
	if broker.publisher == nil {
		broker.ReconnectProducer()
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	partition, offset, err := broker.publisher.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed on send to topic:%v %w", topic, err)
	}
	log.Printf("Order is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

func (broker *Broker) ReceiverConsumer(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	broker.consumer_lock.Lock()
	defer broker.consumer_lock.Unlock()
	if broker.consumer == nil {
		broker.ReconnectConsumer()
	}
	return broker.consumer.ConsumePartition(topic, partition, offset)
}
func (broker *Broker) ClosePublisher() error {
	broker.publisher_lock.Lock()
	defer broker.publisher_lock.Unlock()
	if broker.publisher != nil {
		err := broker.publisher.Close()
		broker.publisher = nil
		if err != nil {
			return err
		}
	}
	return nil
}

func (broker *Broker) CloseConsumer() error {
	broker.consumer_lock.Lock()
	defer broker.consumer_lock.Unlock()
	if broker.consumer != nil {
		err := broker.consumer.Close()
		broker.consumer = nil
		if err != nil {
			return err
		}
	}
	return nil
}
func (broker *Broker) ReconnectProducer() error {
	broker.publisher_lock.Lock()
	defer broker.publisher_lock.Unlock()
	if broker.publisher != nil {
		err := broker.publisher.Close()
		broker.publisher = nil
		if err != nil {
			return fmt.Errorf("error on reconnect producer %w", err)
		}
	}
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 5
	publisher, err := sarama.NewSyncProducer([]string{broker.uri}, config)
	if err != nil {
		return fmt.Errorf("error on create new producer %w", err)
	}
	broker.publisher = publisher
	return nil
}

func (broker *Broker) ReconnectConsumer() error {
	broker.consumer_lock.Lock()
	defer broker.consumer_lock.Unlock()
	if broker.consumer != nil {
		err := broker.consumer.Close()
		broker.consumer = nil
		if err != nil {
			return fmt.Errorf("error on reconnect consumer %w", err)
		}
	}
	consumer_config := sarama.NewConfig()
	consumer_config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{broker.uri}, consumer_config)
	if err != nil {
		return fmt.Errorf("error on create new consumer %w", err)
	}
	broker.consumer = consumer
	return nil
}
