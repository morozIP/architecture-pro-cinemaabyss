package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Глобальные переменные конфигурации, загружаемые из переменных окружения.
var (
	PORT                     string
	MONOLITH_URL             string
	MOVIES_SERVICE_URL       string
	GRADUAL_MIGRATION        bool
	MOVIES_MIGRATION_PERCENT int
)

// Главная функция для запуска прокси-сервиса
func main() {
	log.Println("[PROXY] Starting Proxy Service...")
	StartServer()
}

func init() {
	// Инициализация генератора случайных чисел, начиная с Go 1.20, не требуется,
	// так как стандартная библиотека обеспечивает автоматический сидинг для math/rand.

	// --- Загрузка конфигурации из переменных окружения ---
	PORT = os.Getenv("PORT")
	if PORT == "" {
		PORT = "8000"
	}
	MONOLITH_URL = os.Getenv("MONOLITH_URL")
	MOVIES_SERVICE_URL = os.Getenv("MOVIES_SERVICE_URL")

	// Флаг миграции
	GRADUAL_MIGRATION = os.Getenv("GRADUAL_MIGRATION") == "true"

	// Парсинг процента миграции
	percentStr := os.Getenv("MOVIES_MIGRATION_PERCENT")
	var err error
	MOVIES_MIGRATION_PERCENT, err = strconv.Atoi(percentStr)
	if err != nil || MOVIES_MIGRATION_PERCENT < 0 || MOVIES_MIGRATION_PERCENT > 100 {
		log.Printf("WARN: Invalid or missing MOVIES_MIGRATION_PERCENT ('%s'). Setting to 0.", percentStr)
		MOVIES_MIGRATION_PERCENT = 0
	}

	if MONOLITH_URL == "" || MOVIES_SERVICE_URL == "" {
		log.Fatal("FATAL: MONOLITH_URL and MOVIES_SERVICE_URL must be set.")
	}

	log.Println("--- Proxy Service Configuration ---")
	log.Printf("Listening on port: %s", PORT)
	log.Printf("MONOLITH_URL: %s", MONOLITH_URL)
	log.Printf("MOVIES_SERVICE_URL: %s", MOVIES_SERVICE_URL)
	log.Printf("GRADUAL_MIGRATION enabled: %t", GRADUAL_MIGRATION)
	log.Printf("MOVIES_MIGRATION_PERCENT: %d%%", MOVIES_MIGRATION_PERCENT)
	log.Println("-----------------------------------")
}

// StranglerFigProxyHandler обрабатывает маршрутизацию и выполняет реверс-проксирование.
func StranglerFigProxyHandler(w http.ResponseWriter, r *http.Request) {
	var targetURLStr string
	var routeTag string

	// 1. Маршрут по умолчанию: все запросы направляются на Монолит.
	targetURLStr = MONOLITH_URL
	routeTag = "MONOLITH (Default)"

	// 2. Логика Strangler Fig для /api/movies
	if strings.HasPrefix(r.URL.Path, "/api/movies") {
		// Применяем балансировку, только если флаг миграции активен и процент > 0.
		if GRADUAL_MIGRATION && MOVIES_MIGRATION_PERCENT > 0 {
			// Генерируем случайное число от 0 до 99.
			randNum := rand.Intn(100)

			if randNum < MOVIES_MIGRATION_PERCENT {
				// Запрос направляется на новый Movies Service
				targetURLStr = MOVIES_SERVICE_URL
				routeTag = "MOVIES_SERVICE (Migrated)"
			} else {
				// Запрос остается на Монолите (Legacy)
				targetURLStr = MONOLITH_URL
				routeTag = "MONOLITH (Legacy)"
			}
		} else {
			// Если миграция отключена или 0%, оставляем на Монолите
			routeTag = "MONOLITH (Migration Disabled)"
		}
	}

	// Устанавливаем заголовок ответа для отладки и тестирования маршрута.
	w.Header().Set("X-Proxy-Route", routeTag)

	// Логируем принятое решение о маршрутизации.
	log.Printf("[PROXY] %s %s -> Route: %s, Target: %s", r.Method, r.URL.Path, routeTag, targetURLStr)

	// 3. Подготовка и выполнение Reverse Proxy.
	targetURL, err := url.Parse(targetURLStr)
	if err != nil {
		log.Printf("[FATAL] Invalid target URL configured: %s, Error: %v", targetURLStr, err)
		http.Error(w, "Internal Server Error: Invalid target configuration", http.StatusInternalServerError)
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(targetURL)

	// Устанавливаем таймауты для предотвращения зависания запросов (Улучшение устойчивости)
	proxy.Transport = &http.Transport{
		ResponseHeaderTimeout: 30 * time.Second,
		IdleConnTimeout:       60 * time.Second,
	}

	// Director корректирует запрос перед отправкой на целевой сервис.
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = targetURL.Scheme
		req.URL.Host = targetURL.Host
		req.Host = targetURL.Host // Установка корректного заголовка Host
	}

	// ErrorHandler обрабатывает ошибки соединения с целевым сервисом, возвращая JSON.
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("[PROXY ERROR] Target %s unreachable for path %s: %v", targetURLStr, r.URL.Path, err)
		w.Header().Set("X-Proxy-Route", "Error-503")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		// Возвращаем клиенту структурированное сообщение об ошибке.
		fmt.Fprintf(w, `{"error": "Service Unavailable", "target": "%s", "details": "The target service failed to respond: %v"}`, targetURLStr, err)
	}

	// Обслуживаем запрос через прокси.
	proxy.ServeHTTP(w, r)
}

// StartServer запускает HTTP-сервер с настроенным обработчиком прокси.
func StartServer() {
	http.HandleFunc("/", StranglerFigProxyHandler)

	// Добавляем отдельный обработчик health check для самого прокси
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"UP","service":"proxy_gateway"}`))
	})

	// Запускаем сервер
	serverAddr := fmt.Sprintf(":%s", PORT)
	log.Printf("[PROXY] Starting server on %s", serverAddr)

	err := http.ListenAndServe(serverAddr, nil)
	if err != nil {
		log.Fatalf("[FATAL] Failed to start server: %v", err)
	}
}
