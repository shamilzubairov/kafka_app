package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	pb "app/proto"
)

/*
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"message": "Hello world"}'
*/

type Event struct {
	UserID  string `json:"user_id"`
	Message string `json:"message"`
}

func main() {
	broker := os.Getenv("KAFKA_BROKERS")
	topic := os.Getenv("KAFKA_TOPIC")

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":          broker,
		"enable.idempotence":         true,  // Гарантирует отсутствие дублей при ретраях брокера
		"acks":                       "all", // Продюсер ждёт подтверждения записи сообщения не только от брокера-лидера партиции, но и от всех актуальных реплик
		"retries":                    5,     // Продюсер сделает до 5 повторных попыток отправить сообщение, если предыдущая попытка завершилась временной ошибкой
		"queue.buffering.max.kbytes": 65536, // Ограничиваем буфер 64 МБ (вместо 1 ГБ по умолчанию)
		"message.timeout.ms":         10000, // Сбрасывать неотправленные сообщения с ошибкой через 10 сек
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}

	// Фоновая обработка системных событий продюсера (ошибки, логи)
	go func() {
		for e := range p.Events() {
			if err, ok := e.(kafka.Error); ok {
				log.Printf("Kafka producer error: %v", err)
			}
		}
	}()

	defer func() {
		unflushed := p.Flush(5000)
		if unflushed > 0 {
			log.Printf("Warning: %d messages were not delivered before shutdown", unflushed)
		}
		p.Close()
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		var event Event
		if err := json.Unmarshal(body, &event); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}

		// Собираем сообщение в Kafka
		userEvent := &pb.UserEvent{
			Timestamp: time.Now().Unix(), // or time.Now().Format(time.RFC3339)
			Action:    "POST",
			Message:   event.Message,
		}
		if event.UserID == "" {
			v7, err := uuid.NewV7()
			if err != nil {
				log.Printf("failed to generate uuid: %v", err)
				http.Error(w, "internal server error", http.StatusInternalServerError)
				return
			}
			userEvent.UserId = v7.String()
		} else {
			userEvent.UserId = event.UserID
		}

		// Сериализуем в бинарный формат []byte
		data, err := proto.Marshal(userEvent)
		if err != nil {
			log.Printf("failed to marshal protobuf: %v", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		// Канал для синхронного подтверждения доставки в HTTP-хендлере
		deliveryChan := make(chan kafka.Event, 1)

		// Асинхронная отправка сообщения во внутреннюю очередь библиотеки
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(data),
			Key:            []byte(fmt.Sprintf("key-%s", userEvent.UserId)), // Опциональный ключ
		}, deliveryChan)

		if err != nil {
			log.Printf("failed to write message to kafka: %v", err)
			http.Error(w, "failed to publish event", http.StatusInternalServerError)
			return
		}

		// Ждем подтверждения от брокера Kafka
		select {
		case e := <-deliveryChan:
			m := e.(*kafka.Message)
			if m.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v", m.TopicPartition.Error)
				http.Error(w, "kafka delivery failed", http.StatusInternalServerError)
				return
			}
		case <-time.After(10 * time.Second):
			log.Println("Kafka delivery timeout")
			http.Error(w, "kafka delivery timeout", http.StatusGatewayTimeout)
			return
		}

		log.Printf("Delivered event %s to %v", userEvent.UserId, topic)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{
			"status":   "queued",
			"event_id": userEvent.UserId,
		})
	})

	server := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// Graceful shutdown при остановке контейнера
	go func() {
		log.Println("Producer HTTP server started on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("Shutting down producer...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}
