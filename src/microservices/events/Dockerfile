FROM golang:1.24-alpine AS builder

WORKDIR /app

# Копируем файлы go.mod и go.sum (если есть)
COPY go.mod .
COPY go.sum* .

# Скачиваем зависимости
RUN go mod download

# Копируем исходный код
COPY . .

# Собираем приложение
RUN CGO_ENABLED=0 GOOS=linux go build -o events-service .

# Создаем минимальный образ
FROM alpine:latest

# Устанавливаем CA-сертификаты для HTTPS соединений
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Копируем бинарный файл из предыдущего этапа
COPY --from=builder /app/events-service .

# Открываем порт
EXPOSE 8082

# Запускаем приложение
CMD ["./events-service"]