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

// redisEventBus implement EventBus cho Redis
// Không export struct này ra ngoài
// Sử dụng NewRedisEventBus để khởi tạo

type redisEventBus struct {
	client      *redis.Client
	ctx         context.Context
	subscriber  sync.Map
	middlewares []MiddlewareFunc
	pubsubs     sync.Map // map[string]*redis.PubSub
}

// NewRedisEventBus khởi tạo event bus mới với Redis
func NewRedisEventBus(addr string) EventBus {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &redisEventBus{
		client: rdb,
		ctx:    context.Background(),
	}
}

func (eb *redisEventBus) Use(mw MiddlewareFunc) {
	eb.middlewares = append(eb.middlewares, mw)
}

func (eb *redisEventBus) applyMiddleware(h HandlerFunc) HandlerFunc {
	for i := len(eb.middlewares) - 1; i >= 0; i-- {
		h = eb.middlewares[i](h)
	}
	return h
}

func (eb *redisEventBus) Publish(topic string, payload any) error {
	msg, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload error: %w", err)
	}
	return eb.client.Publish(eb.ctx, topic, msg).Err()
}

func (eb *redisEventBus) Subscribe(topic string, handler func(message []byte)) (string, error) {
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

func (eb *redisEventBus) Unsubscribe(id string) error {
	if v, ok := eb.pubsubs.Load(id); ok {
		if pubsub, ok2 := v.(*redis.PubSub); ok2 {
			err := pubsub.Close()
			eb.pubsubs.Delete(id)
			return err
		}
	}
	return errors.New("pubsub not found")
}

func (eb *redisEventBus) Receive(topic string, handler HandlerFunc) error {
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

func (eb *redisEventBus) Send(topic string, payload any, timeout time.Duration) (json.RawMessage, error) {
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
