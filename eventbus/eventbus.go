// Package eventbus cung cấp hệ thống event bus sử dụng Redis Pub/Sub.
package eventbus

import (
	"encoding/json"
	"time"
)

// HandlerFunc là hàm xử lý event.
type HandlerFunc func(data json.RawMessage) any

// MiddlewareFunc là middleware cho handler.
type MiddlewareFunc func(next HandlerFunc) HandlerFunc

// EventBus interface chung cho các engine khác nhau
// (Redis, RabbitMQ, ...)
type EventBus interface {
	Publish(topic string, payload any) error
	Subscribe(topic string, handler func(message []byte)) (string, error)
	Unsubscribe(id string) error
	Receive(topic string, handler HandlerFunc) error
	Send(topic string, payload any, timeout time.Duration) (json.RawMessage, error)
	Use(mw MiddlewareFunc)
}

// Envelope đóng gói thông tin event gửi qua eventbus.
type Envelope struct {
	EventName     string          `json:"event_name"`
	Data          json.RawMessage `json:"data"`
	ReplyTo       string          `json:"reply_to,omitempty"`
	CorrelationID string          `json:"correlation_id,omitempty"`
}
