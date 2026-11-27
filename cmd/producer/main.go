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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

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

// --- Метрики Prometheus ---
var eventsPublished = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "producer_events_published_total",
		Help: "Количество событий, отправленных в Kafka",
	},
	[]string{"status"}, // "success" или "error"
)

var httpRequestDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "Время обработки HTTP-запросов в секундах",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	},
	[]string{"method", "path"},
)

// Middleware для автоматического сбора HTTP-метрик
func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Не засоряем метрики опросами самого Prometheus и кубера
		if r.URL.Path == "/metrics" || r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now() // Засекли старт

		next.ServeHTTP(w, r) // Выполнили основной хендлер (например, POST /events)

		// Записали, сколько секунд занял весь запрос
		httpRequestDuration.WithLabelValues(r.Method, r.URL.Path).Observe(time.Since(start).Seconds())
	})
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

	// Эндпоинт для Prometheus
	mux.Handle("GET /metrics", promhttp.Handler())

	// Healthcheck для readinessProbe / livenessProbe
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

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
			eventsPublished.WithLabelValues("error").Inc()
			log.Printf("failed to write message to kafka: %v", err)
			http.Error(w, "failed to publish event", http.StatusInternalServerError)
			return
		}

		// Ждем подтверждения от брокера Kafka
		select {
		case e := <-deliveryChan:
			m := e.(*kafka.Message)
			if m.TopicPartition.Error != nil {
				eventsPublished.WithLabelValues("error").Inc()
				log.Printf("Delivery failed: %v", m.TopicPartition.Error)
				http.Error(w, "kafka delivery failed", http.StatusInternalServerError)
				return
			}
		case <-time.After(10 * time.Second):
			eventsPublished.WithLabelValues("error").Inc()
			log.Println("Kafka delivery timeout")
			http.Error(w, "kafka delivery timeout", http.StatusGatewayTimeout)
			return
		}

		eventsPublished.WithLabelValues("success").Inc()

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
		Handler:      metricsMiddleware(mux),
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
