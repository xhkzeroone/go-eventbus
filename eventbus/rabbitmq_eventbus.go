package eventbus

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitmqEventBus struct {
	conn        *amqp.Connection
	ch          *amqp.Channel
	middlewares []MiddlewareFunc
	subscriber  sync.Map // map[string]chan or replyConsumer
}

func NewRabbitMQEventBus(dsn string) (EventBus, error) {
	conn, err := amqp.Dial(dsn)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	return &rabbitmqEventBus{
		conn: conn,
		ch:   ch,
	}, nil
}

func (eb *rabbitmqEventBus) Use(mw MiddlewareFunc) {
	eb.middlewares = append(eb.middlewares, mw)
}

func (eb *rabbitmqEventBus) applyMiddleware(h HandlerFunc) HandlerFunc {
	for i := len(eb.middlewares) - 1; i >= 0; i-- {
		h = eb.middlewares[i](h)
	}
	return h
}

func (eb *rabbitmqEventBus) Publish(topic string, payload any) error {
	var msg []byte
	switch v := payload.(type) {
	case []byte:
		msg = v
	default:
		var err error
		msg, err = json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal payload error: %w", err)
		}
	}
	return eb.ch.Publish(
		"",    // exchange
		topic, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        msg,
		},
	)
}

type replyConsumer struct {
	id          string
	consumerTag string
	channel     *amqp.Channel
}

func (eb *rabbitmqEventBus) subscribeQueue(queue string, handler func(message []byte)) (string, error) {
	q, err := eb.ch.QueueDeclare(
		queue,
		false, // durable
		true,  // auto-delete
		true,  // exclusive
		false,
		nil,
	)
	if err != nil {
		return "", err
	}

	ch, err := eb.conn.Channel()
	if err != nil {
		return "", err
	}

	consumerTag := uuid.NewString()
	deliveries, err := ch.Consume(
		q.Name,
		consumerTag,
		true, // auto-ack
		true, // exclusive
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		return "", err
	}

	id := uuid.NewString()
	eb.subscriber.Store(id, &replyConsumer{id: id, consumerTag: consumerTag, channel: ch})

	go func() {
		for d := range deliveries {
			handler(d.Body)
		}
	}()

	return id, nil
}

func (eb *rabbitmqEventBus) Subscribe(topic string, handler func(message []byte)) (string, error) {
	q, err := eb.ch.QueueDeclare(
		topic,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false,
		nil,
	)
	if err != nil {
		return "", err
	}

	deliveries, err := eb.ch.Consume(
		q.Name,
		"",    // consumer tag
		true,  // auto-ack
		false, // exclusive
		false,
		false,
		nil,
	)
	if err != nil {
		return "", err
	}

	id := uuid.NewString()
	// Không lưu channel vì không dùng đến
	eb.subscriber.Store(id, true)

	go func() {
		for d := range deliveries {
			handler(d.Body)
		}
	}()

	return id, nil
}

func (eb *rabbitmqEventBus) Unsubscribe(id string) error {
	if v, ok := eb.subscriber.Load(id); ok {
		if rc, ok := v.(*replyConsumer); ok {
			_ = rc.channel.Cancel(rc.consumerTag, false)
			_ = rc.channel.Close()
		}
		eb.subscriber.Delete(id)
	}
	return nil
}

func (eb *rabbitmqEventBus) Receive(topic string, handler HandlerFunc) error {
	finalHandler := eb.applyMiddleware(handler)
	_, err := eb.Subscribe(topic, func(msg []byte) {
		var env Envelope
		if err := json.Unmarshal(msg, &env); err != nil {
			log.Println("error decoding envelope:", err)
			return
		}
		response := finalHandler(env.Data)
		if env.ReplyTo != "" && env.CorrelationID != "" {
			respBytes, err := json.Marshal(response)
			if err != nil {
				log.Println("marshal response error:", err)
				return
			}
			replyEnv := Envelope{
				Data:          respBytes,
				CorrelationID: env.CorrelationID,
			}
			_ = eb.Publish(env.ReplyTo, replyEnv)
		}
	})
	return err
}

func (eb *rabbitmqEventBus) Send(topic string, payload any, timeout time.Duration) (json.RawMessage, error) {
	replyTo := "reply_" + uuid.NewString()
	correlationID := uuid.NewString()

	respChan := make(chan json.RawMessage, 1)
	eb.subscriber.Store(correlationID, respChan)
	defer eb.subscriber.Delete(correlationID)
	defer close(respChan)

	replyConsumerID, err := eb.subscribeQueue(replyTo, func(msg []byte) {
		var env Envelope
		if err := json.Unmarshal(msg, &env); err != nil {
			log.Println("unmarshal reply envelope error:", err)
			return
		}
		if ch, ok := eb.subscriber.Load(env.CorrelationID); ok {
			if c, ok := ch.(chan json.RawMessage); ok {
				select {
				case c <- env.Data:
				default:
					log.Println("warning: response channel is full, dropping reply")
				}
			}
		}
	})
	if err != nil {
		return nil, err
	}
	defer func(eb *rabbitmqEventBus, id string) {
		_ = eb.Unsubscribe(id)
	}(eb, replyConsumerID)

	msgBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload error: %w", err)
	}

	env := Envelope{
		EventName:     topic,
		Data:          msgBytes,
		ReplyTo:       replyTo,
		CorrelationID: correlationID,
	}

	envBytes, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("marshal envelope error: %w", err)
	}

	if err := eb.Publish(topic, envBytes); err != nil {
		return nil, err
	}

	select {
	case resp := <-respChan:
		return resp, nil
	case <-time.After(timeout):
		return nil, errors.New("timeout waiting for reply")
	}
}
