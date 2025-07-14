// Package eventbus cung cấp hệ thống event bus sử dụng Redis Pub/Sub.
package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Envelope đóng gói thông tin event gửi qua eventbus.
type Envelope struct {
	EventName     string          `json:"event_name"`
	Data          json.RawMessage `json:"data"`
	ReplyTo       string          `json:"reply_to,omitempty"`
	CorrelationID string          `json:"correlation_id,omitempty"`
}

// HandlerFunc là hàm xử lý event.
type HandlerFunc func(data json.RawMessage) any

// MiddlewareFunc là middleware cho handler.
type MiddlewareFunc func(next HandlerFunc) HandlerFunc

// EventBus quản lý publish/subscribe event qua Redis.
type EventBus struct {
	client      *redis.Client
	ctx         context.Context
	subscriber  sync.Map
	middlewares []MiddlewareFunc
	// Lưu các pubsub để unsubscribe khi cần
	pubsubs sync.Map // map[string]*redis.PubSub
}

// NewEventBus khởi tạo event bus mới với địa chỉ Redis.
func NewEventBus(addr string) *EventBus {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	return &EventBus{
		client: rdb,
		ctx:    context.Background(),
	}
}

// Use thêm middleware vào event bus.
func (eb *EventBus) Use(mw MiddlewareFunc) {
	eb.middlewares = append(eb.middlewares, mw)
}

// applyMiddleware áp dụng middleware cho handler.
func (eb *EventBus) applyMiddleware(h HandlerFunc) HandlerFunc {
	for i := len(eb.middlewares) - 1; i >= 0; i-- {
		h = eb.middlewares[i](h)
	}
	return h
}

// Publish gửi event tới topic.
func (eb *EventBus) Publish(topic string, payload any) error {
	msg, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload error: %w", err)
	}

	return eb.client.Publish(eb.ctx, topic, msg).Err()
}

// Subscribe đăng ký nhận event từ topic. Trả về id để unsubscribe.
func (eb *EventBus) Subscribe(topic string, handler func(message []byte)) (string, error) {
	pubsub := eb.client.Subscribe(eb.ctx, topic)

	_, err := pubsub.Receive(eb.ctx)
	if err != nil {
		return "", err
	}

	ch := pubsub.Channel()
	id := uuid.NewString()
	eb.pubsubs.Store(id, pubsub)

	go func() {
		for msg := range ch {
			handler([]byte(msg.Payload))
		}
	}()

	return id, nil
}

// Unsubscribe hủy đăng ký nhận event theo id trả về từ Subscribe.
func (eb *EventBus) Unsubscribe(id string) error {
	if v, ok := eb.pubsubs.Load(id); ok {
		if pubsub, ok2 := v.(*redis.PubSub); ok2 {
			err := pubsub.Close()
			eb.pubsubs.Delete(id)
			return err
		}
	}
	return errors.New("pubsub not found")
}

// Handle đăng ký handler cho topic, áp dụng middleware.
func (eb *EventBus) Handle(topic string, handler HandlerFunc) error {
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
			replyMsg, err := json.Marshal(replyEnv)
			if err != nil {
				log.Println("marshal reply envelope error:", err)
				return
			}
			_ = eb.client.Publish(eb.ctx, env.ReplyTo, replyMsg).Err()
		}
	})
	return err
}

// Send gửi request tới topic và chờ phản hồi (request-reply pattern).
func (eb *EventBus) Send(topic string, payload any, timeout time.Duration) (json.RawMessage, error) {
	replyTo := "reply_" + uuid.NewString()
	correlationID := uuid.NewString()

	respChan := make(chan json.RawMessage, 1)
	eb.subscriber.Store(correlationID, respChan)

	handleFunc := func(msg []byte) {
		var env Envelope
		if err := json.Unmarshal(msg, &env); err != nil {
			log.Println("unmarshal reply envelope error:", err)
			return
		}
		if ch, ok := eb.subscriber.Load(env.CorrelationID); ok {
			if c, ok := ch.(chan json.RawMessage); ok {
				c <- env.Data
			}
		}
	}
	// Đăng ký nhận reply, lưu lại id để unsubscribe sau
	id, err := eb.Subscribe(replyTo, handleFunc)
	if err != nil {
		return nil, err
	}

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
	if err := eb.client.Publish(eb.ctx, topic, envBytes).Err(); err != nil {
		return nil, err
	}

	var resp json.RawMessage
	select {
	case resp = <-respChan:
		eb.subscriber.Delete(correlationID)
	case <-time.After(timeout):
		eb.subscriber.Delete(correlationID)
		_ = eb.Unsubscribe(id)
		return nil, errors.New("timeout waiting for reply")
	}
	_ = eb.Unsubscribe(id)
	return resp, nil
}
