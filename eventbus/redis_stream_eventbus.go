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

type redisStreamEventBus struct {
	client      *redis.Client
	ctx         context.Context
	groupName   string
	middlewares []MiddlewareFunc
	wg          sync.WaitGroup
	cancelFuncs sync.Map // map[string]context.CancelFunc
}

func NewRedisStreamEventBus(addr, groupName string) EventBus {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &redisStreamEventBus{
		client:    rdb,
		ctx:       context.Background(),
		groupName: groupName,
	}
}

func (eb *redisStreamEventBus) Use(mw MiddlewareFunc) {
	eb.middlewares = append(eb.middlewares, mw)
}

func (eb *redisStreamEventBus) applyMiddleware(h HandlerFunc) HandlerFunc {
	for i := len(eb.middlewares) - 1; i >= 0; i-- {
		h = eb.middlewares[i](h)
	}
	return h
}

func (eb *redisStreamEventBus) Publish(topic string, payload any) error {
	msgBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload error: %w", err)
	}
	data := map[string]interface{}{
		"data": msgBytes,
	}
	return eb.client.XAdd(eb.ctx, &redis.XAddArgs{
		Stream: topic,
		Values: data,
	}).Err()
}

func (eb *redisStreamEventBus) Subscribe(topic string, handler func(msg []byte)) (string, error) {
	_ = eb.client.XGroupCreateMkStream(eb.ctx, topic, eb.groupName, "$")

	consumerID := uuid.NewString()
	ctx, cancel := context.WithCancel(context.Background())
	eb.cancelFuncs.Store(consumerID, cancel)

	eb.wg.Add(1)
	go func() {
		defer eb.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				streams, err := eb.client.XReadGroup(ctx, &redis.XReadGroupArgs{
					Group:    eb.groupName,
					Consumer: consumerID,
					Streams:  []string{topic, ">"},
					Block:    5 * time.Second,
					Count:    1,
				}).Result()
				if err != nil && !errors.Is(err, redis.Nil) {
					log.Println("XReadGroup error:", err)
					continue
				}
				for _, stream := range streams {
					for _, msg := range stream.Messages {
						rawAny, ok := msg.Values["data"]
						if !ok {
							log.Println("missing 'data' field:", msg.ID)
							continue
						}
						rawStr, ok := rawAny.(string)
						if !ok {
							log.Println("invalid 'data' field:", msg.ID)
							continue
						}
						handler([]byte(rawStr))
						_ = eb.client.XAck(ctx, topic, eb.groupName, msg.ID)
					}
				}
			}
		}
	}()

	return consumerID, nil
}

// SubscribeStream Nếu cần dùng msgID, tạo hàm riêng (không implement interface)
func (eb *redisStreamEventBus) SubscribeStream(topic string, handler func(msgID string, data []byte)) (string, error) {
	_ = eb.client.XGroupCreateMkStream(eb.ctx, topic, eb.groupName, "$")

	consumerID := uuid.NewString()
	ctx, cancel := context.WithCancel(context.Background())
	eb.cancelFuncs.Store(consumerID, cancel)

	eb.wg.Add(1)
	go func() {
		defer eb.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				streams, err := eb.client.XReadGroup(ctx, &redis.XReadGroupArgs{
					Group:    eb.groupName,
					Consumer: consumerID,
					Streams:  []string{topic, ">"},
					Block:    5 * time.Second,
					Count:    1,
				}).Result()
				if err != nil && !errors.Is(err, redis.Nil) {
					log.Println("XReadGroup error:", err)
					continue
				}
				for _, stream := range streams {
					for _, msg := range stream.Messages {
						rawAny, ok := msg.Values["data"]
						if !ok {
							log.Println("missing 'data' field:", msg.ID)
							continue
						}
						rawStr, ok := rawAny.(string)
						if !ok {
							log.Println("invalid 'data' field:", msg.ID)
							continue
						}
						handler(msg.ID, []byte(rawStr))
						_ = eb.client.XAck(ctx, topic, eb.groupName, msg.ID)
					}
				}
			}
		}
	}()

	return consumerID, nil
}

func (eb *redisStreamEventBus) Unsubscribe(consumerID string) error {
	if cancel, ok := eb.cancelFuncs.Load(consumerID); ok {
		if fn, ok := cancel.(context.CancelFunc); ok {
			fn()
		}
		eb.cancelFuncs.Delete(consumerID)
	}
	return nil
}

func (eb *redisStreamEventBus) Receive(topic string, handler HandlerFunc) error {
	finalHandler := eb.applyMiddleware(handler)
	_, err := eb.Subscribe(topic, func(msg []byte) {
		var env Envelope
		if err := json.Unmarshal(msg, &env); err != nil {
			log.Println("unmarshal error:", err)
			return
		}
		resp := finalHandler(env.Data)
		if env.ReplyTo != "" && env.CorrelationID != "" {
			respBytes, _ := json.Marshal(resp)
			replyEnv := Envelope{
				Data:          respBytes,
				CorrelationID: env.CorrelationID,
			}
			_ = eb.Publish(env.ReplyTo, replyEnv)
		}
	})
	return err
}

func (eb *redisStreamEventBus) Send(topic string, payload any, timeout time.Duration) (json.RawMessage, error) {
	replyTo := "reply_" + uuid.NewString()
	correlationID := uuid.NewString()
	respChan := make(chan struct {
		data  json.RawMessage
		msgID string
	}, 1)

	// Đăng ký nhận reply (dùng SubscribeStream để lấy msgID)
	subID, err := eb.SubscribeStream(replyTo, func(msgID string, msg []byte) {
		var env Envelope
		if err := json.Unmarshal(msg, &env); err != nil {
			log.Println("unmarshal reply error:", err)
			return
		}
		if env.CorrelationID == correlationID {
			respChan <- struct {
				data  json.RawMessage
				msgID string
			}{env.Data, msgID}
		}
	})
	if err != nil {
		close(respChan)
		return nil, err
	}
	defer func() {
		_ = eb.Unsubscribe(subID)
		close(respChan)
	}()

	msgBytes, _ := json.Marshal(payload)
	env := Envelope{
		EventName:     topic,
		Data:          msgBytes,
		ReplyTo:       replyTo,
		CorrelationID: correlationID,
	}
	_ = eb.Publish(topic, env)

	select {
	case resp := <-respChan:
		if resp.msgID != "" {
			_ = eb.client.XAck(eb.ctx, replyTo, eb.groupName, resp.msgID)
			_ = eb.client.XDel(eb.ctx, replyTo, resp.msgID)
		}
		return resp.data, nil
	case <-time.After(timeout):
		return nil, errors.New("timeout waiting for reply")
	}
}
