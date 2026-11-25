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
		"bootstrap.servers":  broker,
		"enable.idempotence": true, // Гарантирует отсутствие дублей при ретраях брокера
		"acks":               "all",
		"retries":            5,
	})
	if err != nil {
		panic(err)
	}

	defer func() {
		p.Flush(5000)
		p.Close()
	}()

	userEvent := &pb.UserEvent{}

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
		userEvent.Timestamp = time.Now().Unix() // or time.Now().Format(time.RFC3339)
		if event.UserID == "" {
			v7, err := uuid.NewV7()
			if err != nil {
				log.Fatal(err)
			}
			userEvent.UserId = v7.String()
		} else {
			userEvent.UserId = event.UserID
		}
		userEvent.Action = "POST"
		userEvent.Message = event.Message

		// Сериализуем в бинарный формат []byte
		data, err := proto.Marshal(userEvent)
		if err != nil {
			log.Fatalf("failed to marshal protobuf: %v", err)
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
		e := <-deliveryChan
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			log.Printf("Delivery failed: %v", m.TopicPartition.Error)
			http.Error(w, "kafka delivery failed", http.StatusInternalServerError)
			return
		}

		log.Printf("Delivered event %s to %v", userEvent.UserId, m.TopicPartition)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{
			"status":   "queued",
			"event_id": userEvent.UserId,
		})
	})

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
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
