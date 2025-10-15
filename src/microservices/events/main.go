package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// Карта, связывающая тип события с его целевым топиком Kafka.
var topicMap = map[string]string{
	"movie":   "movie-events",
	"user":    "user-events",
	"payment": "payment-events",
}

// Список всех топиков, из которых должен читать данный сервис (для Consumer).
var consumerTopics = []string{
	"movie-events",
	"user-events",
	"payment-events",
}

// --- 1. Структуры данных (Модели событий согласно OpenAPI) ---

// EventResponse - ответ, который мы отправляем клиенту
type EventResponse struct {
	Status    string `json:"status"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

// EventPayload - общая структура, которую мы сериализуем и отправляем в Kafka
type EventPayload struct {
	Type      string      `json:"type"`      // movie, user, payment
	Timestamp time.Time   `json:"timestamp"` // Время создания
	Data      interface{} `json:"data"`      // Фактические данные события
}

// MovieEvent, UserEvent, PaymentEvent - структуры, соответствующие входным данным API
type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      int      `json:"user_id,omitempty"`
	Rating      float64  `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description string   `json:"description,omitempty"`
}

type UserEvent struct {
	UserID    int       `json:"user_id"`
	Username  string    `json:"username,omitempty"`
	Email     string    `json:"email,omitempty"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"` // Будем использовать время из запроса, если есть
}

type PaymentEvent struct {
	PaymentID  int       `json:"payment_id"`
	UserID     int       `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	MethodType string    `json:"method_type,omitempty"`
}

// --- 2. Kafka Producer и Consumer ---

var (
	writer *kafka.Writer // Продюсер для отправки сообщений
	// Используем KAFKA_BROKERS из окружения
	KAFKA_BROKERS string
	SERVICE_PORT  string
)

func init() {
	// Читаем KAFKA_BROKERS из окружения
	KAFKA_BROKERS = os.Getenv("KAFKA_BROKERS")
	if KAFKA_BROKERS == "" {
		log.Fatal("FATAL: KAFKA_BROKERS must be set (e.g., kafka:9092).")
	}

	SERVICE_PORT = os.Getenv("PORT")
	if SERVICE_PORT == "" {
		SERVICE_PORT = "8082"
	}

	log.Printf("INFO: Starting with KAFKA_BROKERS: %s and PORT: %s", KAFKA_BROKERS, SERVICE_PORT)

	// Инициализируем Kafka Producer (Writer)
	writer = NewProducer()
}

// NewProducer создает и возвращает Writer для Kafka, не привязанный к конкретному топику.
func NewProducer() *kafka.Writer {
	log.Printf("INFO: Initializing Kafka Writer for dynamic topics...")
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:      strings.Split(KAFKA_BROKERS, ","),
		Topic:        "", // Топик не указан, будет установлен в produceEvent
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
		// Используем идиоматическую константу
		RequiredAcks: int(kafka.RequireAll),
	})
}

// produceEvent - универсальная функция для отправки любого события в Kafka.
func produceEvent(ctx context.Context, eventType string, eventData interface{}) (string, int, int64, error) {
	// Получаем имя топика по типу события
	topic, ok := topicMap[eventType]
	if !ok {
		return "", 0, 0, fmt.Errorf("unknown event type: %s", eventType)
	}

	// Оборачиваем данные в общую структуру
	payload := EventPayload{
		Type:      eventType,
		Timestamp: time.Now().UTC(),
		Data:      eventData,
	}

	// Сериализуем общую структуру в JSON для Kafka
	value, err := json.Marshal(payload)
	if err != nil {
		return topic, 0, 0, fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Создаем Kafka Message
	msg := kafka.Message{
		Topic: topic, // Динамически устанавливаем топик
		// Key помогает гарантировать порядок сообщений внутри одной партиции
		Key:   []byte(fmt.Sprintf("%s-%d", eventType, time.Now().UnixNano())),
		Value: value,
	}

	// Отправляем сообщение
	// writer.WriteMessages теперь возвращает только error в последних версиях
	if err := writer.WriteMessages(ctx, msg); err != nil {
		return topic, 0, 0, fmt.Errorf("failed to write message to Kafka: %w", err)
	}

	// Возвращаем имя топика и заглушки для Partition/Offset (в синхронном режиме не доступны)
	return topic, 0, 0, nil
}

// StartTopicConsumer запускает консьюмер для конкретного топика в отдельной горутине.
func StartTopicConsumer(ctx context.Context, topic string) {
	// Создаем Reader для Kafka
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(KAFKA_BROKERS, ","),
		Topic:   topic,                    // Читаем только из своего топика
		GroupID: "events-service-group-1", // Общая группа для всех топиков
		// Читаем с самого начала топика при первом запуске
		StartOffset: kafka.FirstOffset,
	})

	log.Printf("INFO: Starting Kafka Reader for topic '%s' in group 'events-service-group-1'...", topic)

	for {
		select {
		case <-ctx.Done():
			log.Printf("INFO: Consumer for topic %s shutting down...", topic)
			reader.Close()
			return
		default:
			// Читаем следующее сообщение
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				// Если ошибка - логируем и продолжаем, кроме случаев остановки.
				log.Printf("ERROR: Kafka ReadMessage failed on topic %s: %v", topic, err)
				time.Sleep(1 * time.Second)
				continue
			}

			// Десериализуем payload
			var payload EventPayload
			if err := json.Unmarshal(m.Value, &payload); err != nil {
				log.Printf("ERROR: Failed to unmarshal message payload on topic %s: %v", topic, err)
				continue
			}

			// Логируем обработанное событие
			log.Printf("CONSUMED: Topic: %s, Partition: %d, Offset: %d, Type: %s, Key: %s",
				m.Topic, m.Partition, m.Offset, payload.Type, string(m.Key))

			// Подтверждаем, что сообщение обработано
			reader.CommitMessages(ctx, m)
		}
	}
}

// StartConsumers запускает консьюмер для каждого определенного топика.
func StartConsumers(ctx context.Context) {
	for _, topic := range consumerTopics {
		go StartTopicConsumer(ctx, topic)
	}
}

// --- 3. HTTP Handlers (API) ---

// BaseHandler является универсальным обработчиком для всех типов событий
func BaseHandler(eventType string, w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// 1. Десериализация данных
	var eventData interface{}
	switch eventType {
	case "movie":
		eventData = &MovieEvent{}
	case "user":
		eventData = &UserEvent{}
	case "payment":
		eventData = &PaymentEvent{}
	default:
		// Если это неизвестный тип, возвращаем ошибку
		http.Error(w, `{"error":"Unknown event type"}`, http.StatusInternalServerError)
		return
	}

	if err := json.NewDecoder(r.Body).Decode(&eventData); err != nil {
		log.Printf("ERROR: Failed to decode JSON for %s event: %v", eventType, err)
		http.Error(w, `{"error":"Invalid JSON payload"}`, http.StatusBadRequest)
		return
	}

	// 2. Отправка сообщения в Kafka
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	topic, partition, offset, err := produceEvent(ctx, eventType, eventData)
	if err != nil {
		log.Printf("ERROR: Failed to produce message for %s event to topic %s: %v", eventType, topic, err)
		http.Error(w, fmt.Sprintf(`{"error":"Failed to send to Kafka: %s"}`, err.Error()), http.StatusServiceUnavailable)
		return
	}

	// 3. Отправка успешного ответа
	w.WriteHeader(http.StatusCreated)
	resp := EventResponse{
		Status:    "success",
		Topic:     topic, // Добавляем имя топика в ответ
		Partition: partition,
		Offset:    offset,
	}

	json.NewEncoder(w).Encode(resp)
	log.Printf("PRODUCED: Successfully sent %s event to topic %s.", eventType, topic)
}

// HandleMovieEvent обрабатывает POST /api/events/movie
func HandleMovieEvent(w http.ResponseWriter, r *http.Request) {
	BaseHandler("movie", w, r)
}

// HandleUserEvent обрабатывает POST /api/events/user
func HandleUserEvent(w http.ResponseWriter, r *http.Request) {
	BaseHandler("user", w, r)
}

// HandlePaymentEvent обрабатывает POST /api/events/payment
func HandlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	BaseHandler("payment", w, r)
}

// HandleHealth проверяет работоспособность сервиса
func HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

// --- 4. Запуск Сервера ---

func main() {
	// Создаем контекст для управления консьюмерами
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Запускаем Kafka Consumers (по одному на каждый топик)
	StartConsumers(ctx)

	// 2. Настраиваем HTTP роуты
	http.HandleFunc("/api/events/health", HandleHealth)
	http.HandleFunc("/api/events/movie", HandleMovieEvent)
	http.HandleFunc("/api/events/user", HandleUserEvent)
	http.HandleFunc("/api/events/payment", HandlePaymentEvent)

	// 3. Запускаем HTTP-сервер
	serverAddr := fmt.Sprintf(":%s", SERVICE_PORT)
	log.Printf("INFO: HTTP Server listening on %s", serverAddr)

	if err := http.ListenAndServe(serverAddr, nil); err != nil {
		log.Fatalf("FATAL: HTTP Server failed: %v", err)
	}

	// Закрываем продюсер при завершении
	if err := writer.Close(); err != nil {
		log.Printf("ERROR: Failed to close Kafka writer: %v", err)
	}
}
