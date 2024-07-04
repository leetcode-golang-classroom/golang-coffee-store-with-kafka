package application

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/IBM/sarama"
	"github.com/leetcode-golang-classroom/golang-coffee-store-with-kafka/internal/broker"
)

type OrderWorker struct {
	broker *broker.Broker
	sync.RWMutex
}

func NewWorker(broker *broker.Broker) *OrderWorker {
	return &OrderWorker{
		broker: broker,
	}
}

func (orderWorker *OrderWorker) Run(ctx context.Context) error {
	orderWorker.Lock()
	defer orderWorker.Unlock()
	topic := "coffee_orders"
	msgCnt := 0
	consumer, err := orderWorker.broker.ReceiverConsumer(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Printf("consumer consumer err %v\n", err)
		return err
	}
	log.Println("consumer start")
	for {
		select {
		case err := <-consumer.Errors():
			fmt.Println(err)
		case msg := <-consumer.Messages():
			msgCnt++
			fmt.Printf("Received order Count %d: | Topic(%s) | Message(%s)\n", msgCnt, string(msg.Topic), string(msg.Value))
			order := string(msg.Value)
			fmt.Printf("Brewing coffee for order %v\n", order)
		case <-ctx.Done():
			log.Printf("consumer end\n")
			log.Printf("Processed %d messages\n", msgCnt)
			return nil
		}
	}
}
