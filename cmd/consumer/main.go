package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	pb "app/proto"
)

// Структура для ответа на GET /events
type EventRecord struct {
	UserID  string `json:"user_id"`
	Action  string `json:"action"`
	Message string `json:"message"`
}

func main() {
	broker := os.Getenv("KAFKA_BROKERS")
	groupID := "group-1"
	topic := os.Getenv("KAFKA_TOPIC")
	dbDSN := os.Getenv("DB_DSN")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dbDSN)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer pool.Close()

	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS processed_events (
			event_id UUID PRIMARY KEY,
			processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE TABLE IF NOT EXISTS events (
			id SERIAL PRIMARY KEY,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			user_id TEXT NOT NULL,
			action TEXT NOT NULL,
			message TEXT NOT NULL
		);
	`)

	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// 2. Запускаем HTTP-сервер для GET /events в отдельной горутине
	mux := http.NewServeMux()
	mux.HandleFunc("GET /events", getEventsHandler(pool))

	httpServer := &http.Server{
		Addr:    ":8082",
		Handler: mux,
	}

	go func() {
		log.Println("Consumer HTTP API started on :8082")
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  broker,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest", // Читать с начала, если нет смещения
		"enable.auto.commit": false,      // Управляем коммитами сами после tx.Commit()
		"isolation.level":    "read_committed",
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error consumer: %v\n", err)
		os.Exit(1)
	}

	// Подписка на топик
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error subscribe: %v\n", err)
		os.Exit(1)
	}

	log.Println("Confluent Consumer with Exactly-Once started...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Consumer shutting down...")
			return
		default:
			// Читаем сообщение с таймаутом 1 секунда, чтобы проверять ctx.Done()
			msg, err := c.ReadMessage(1 * time.Second)
			if err != nil {
				var kafkaErr kafka.Error
				if errors.As(err, &kafkaErr) && kafkaErr.Code() == kafka.ErrAllBrokersDown {
					fmt.Fprintf(os.Stderr, "All brokers down error: %v\n", err)
					return
				}
				// Корректно отлавливаем таймаут (когда просто нет новых сообщений) и идем на следующий круг
				if errors.As(err, &kafkaErr) && kafkaErr.Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("Consumer read error: %v", err)
				continue
			}

			// Обрабатываем в транзакции БД (Дедупликация + Бизнес-логика)
			if err := processMessage(ctx, pool, msg.Value); err != nil {
				log.Printf("DB processing failed (will retry): %v", err)
				// При ошибке не коммитим offset и перематываем позицию назад,
				// чтобы librdkafka снова отдал это же сообщение
				_ = c.Seek(msg.TopicPartition, 1000)
				time.Sleep(time.Second)
				continue
			}

			// Фиксируем offset в Kafka только ПОСЛЕ успешной транзакции в Postgres
			if _, err := c.CommitMessage(msg); err != nil {
				log.Printf("Failed to commit Kafka offset: %v", err)
			}
		}
	}
}

// HTTP-хендлер для получения всех записей из таблицы events
func getEventsHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rows, err := pool.Query(r.Context(), `SELECT user_id, action, message FROM events`)
		if err != nil {
			log.Printf("Query events error: %v", err)
			http.Error(w, "failed to fetch events", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		// Инициализируем пустой срез, чтобы при отсутствии записей вернулся [], а не null
		events := make([]EventRecord, 0)

		for rows.Next() {
			var ev EventRecord
			if err := rows.Scan(&ev.UserID, &ev.Action, &ev.Message); err != nil {
				log.Printf("Scan row error: %v", err)
				http.Error(w, "failed to scan events", http.StatusInternalServerError)
				return
			}
			events = append(events, ev)
		}

		if err := rows.Err(); err != nil {
			log.Printf("Rows iteration error: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(events); err != nil {
			log.Printf("JSON encode error: %v", err)
		}
	}
}

func processMessage(ctx context.Context, pool *pgxpool.Pool, raw []byte) error {
	var event pb.UserEvent
	if err := proto.Unmarshal(raw, &event); err != nil {
		log.Printf("failed to unmarshal protobuf: %v", err)
		return nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	fmt.Println(event.Message, event.UserId)

	// 1. Дедупликация (Exactly-Once)
	tag, err := tx.Exec(ctx, `
		INSERT INTO processed_events (event_id)
		VALUES ($1)
		ON CONFLICT (event_id) DO NOTHING
	`, event.UserId)
	if err != nil {
		return fmt.Errorf("insert processed_events: %w", err)
	}

	// количество строк в базе данных, которые были реально вставлены,
	// изменены или удалены выполненным SQL-запросом (INSERT, UPDATE или DELETE).
	if tag.RowsAffected() == 0 {
		// Сообщение уже обрабатывалось ранее!
		// PostgreSQL проигнорировал вставку, затронув 0 строк.
		// Мы понимаем, что это дубль, и безопасно пропускаем его без лишнего запроса SELECT.
		log.Printf("Duplicate event %s skipped", event.UserId)
		return tx.Commit(ctx)
	}

	// 2. Бизнес-запись
	_, err = tx.Exec(ctx, `
		INSERT INTO events (user_id, action, message)
		VALUES ($1, $2, $3)
	`, event.UserId, event.Action, event.Message)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	log.Printf("Processed event %s -> saved to DB", event.UserId)
	return nil
}
