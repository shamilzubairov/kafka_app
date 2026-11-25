# --- ЭТАП 1: Базовый образ и зависимости (кэшируется навсегда) ---
FROM golang:1.26-bookworm AS deps

# Устанавливаем C-компилятор (нужен для CGO)
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем ТОЛЬКО файлы зависимостей
COPY go.mod go.sum ./
# Скачиваем библиотеки. Этот слой закэшируется, пока вы не измените go.mod
RUN go mod download

# --- ЭТАП 2: Сборка вашего кода (выполняется при каждом изменении .go) ---
FROM deps AS builder
# Копируем остальной исходный код
COPY . .

# Передаем имя сервиса при сборке (producer или consumer)
ARG SERVICE_NAME
# Включаем CGO_ENABLED=1 (библиотека confluent-kafka-go поставляется со встроенным librdkafka)
RUN CGO_ENABLED=1 GOOS=linux go build -o /app/service ./cmd/${SERVICE_NAME}

# --- ЭТАП 3: Финальный лёгкий образ для запуска ---
FROM debian:bookworm-slim

# Устанавливаем корневые сертификаты (нужны для TLS/SASL подключения к Kafka)
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/service .

CMD ["./service"]
