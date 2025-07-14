package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/xhkzeroone/go-eventbus/eventbus"
)

type AddRequest struct {
	A int `json:"a"`
	B int `json:"b"`
}

type AddResponse struct {
	Result int `json:"result"`
}

func LoggingMiddleware(next eventbus.HandlerFunc) eventbus.HandlerFunc {
	return func(data json.RawMessage) any {
		fmt.Println("📥 Nhận message:", string(data))
		res := next(data)
		fmt.Println("📤 Phản hồi:", res)
		return res
	}
}

func RejectEmptyMiddleware(next eventbus.HandlerFunc) eventbus.HandlerFunc {
	return func(data json.RawMessage) any {
		if len(data) == 0 {
			fmt.Println("❌ Message rỗng, bị chặn.")
			return map[string]any{"error": "empty payload"}
		}
		return next(data)
	}
}

func main() {
	var bus eventbus.EventBus

	bus = eventbus.NewRedisEventBus("localhost:6379")
	fmt.Println("[EventBus] Sử dụng Redis")

	//var err error
	//bus, err = eventbus.NewRabbitMQEventBus("amqp://guest:guest@localhost:5672/")
	//if err != nil {
	//	fmt.Println("Không kết nối được RabbitMQ:", err)
	//	return
	//}

	// Gắn middleware
	bus.Use(LoggingMiddleware)
	bus.Use(RejectEmptyMiddleware)

	// Đăng ký handler cho math.add
	bus.Receive("math.add", func(data json.RawMessage) any {
		var req AddRequest
		_ = json.Unmarshal(data, &req)
		return AddResponse{Result: req.A + req.B}
	})

	// Gửi request demo
	go func() {
		req := AddRequest{A: 10, B: 20}
		resp, err := bus.Send("math.add", req, 3*time.Second)
		if err != nil {
			fmt.Println("❌ Lỗi gửi request:", err)
			return
		}
		var res AddResponse
		_ = json.Unmarshal(resp, &res)
		fmt.Println("✅ Tổng:", res.Result)
	}()

	time.Sleep(5 * time.Second)
}
