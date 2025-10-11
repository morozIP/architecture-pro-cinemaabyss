```puml
@startuml
title Kinobezdna To-Be Container Diagram

top to bottom direction

!includeurl https://raw.githubusercontent.com/RicardoNiepel/C4-PlantUML/master/C4_Container.puml

Person(user, "Пользователь", "Зритель на ноутбуке, смарт ТВ или мобильном устройстве")

System_Ext(payment_system, "Платёжная система", "Внешняя система для процессинга платежей")
System_Ext(ext_cinemas, "Онлайн-кинотеатры", "Внешние источники контента")
System_Ext(ext_recommender, "Внешняя рекомендательная система", "Сторонняя система для подбора контента")

System_Boundary(kinobezdna_platform, "Платформа «Кинобездна» (Kubernetes)") {

    Container(api_gateway, "API Gateway", "Go / Kubernetes", "Единая точка входа, маршрутизация запросов от клиентов.")

    Container(user_service, "User & Subscription Service", "Go", "Управление пользователями, аутентификация, подписки, платежи.")
    Container(metadata_service, "Metadata Service (Movies)", "Go", "Метаданные фильмов: жанры, актёры, оценки, описание.")
    Container(catalog_service, "Catalog Service", "Go", "Управление доступным контентом, агрегация данных из внешних кинотеатров.")
    Container(loyalty_service, "Loyalty/Discount Service", "Go", "Управление скидками, промокодами, лояльностью.")

    Container(kafka, "Kafka", "Cluster", "Брокер сообщений для асинхронного взаимодействия между микросервисами.")
    Container(rabbitmq, "RabbitMQ", "Cluster", "Брокер сообщений для обмена с внешней рекомендательной системой.")
    
    ContainerDb(db_user, "PostgreSQL (PG)", "База данных User Service", "Пользователи, подписки, платежи, избранное.")
    ContainerDb(db_meta, "PostgreSQL (PG)", "База данных Metadata Service", "Метаданные фильмов.")
    ContainerDb(db_catalog, "PostgreSQL (PG)", "База данных Catalog Service", "Индексы и доступность контента.")
    
    Container(object_storage, "S3-совместимое хранилище", "S3", "Хранение медиа-файлов, постеров.")
}

Rel(user, api_gateway, "REST API (HTTP/S)", "Весь клиентский трафик")

' Синхронное взаимодействие (через API Gateway)
Rel(api_gateway, user_service, "Запросы аутентификации/профиля", "REST/gRPC")
Rel(api_gateway, metadata_service, "Запросы данных о фильмах/сериалах", "REST/gRPC")
Rel(api_gateway, catalog_service, "Запросы каталога контента", "REST/gRPC")
Rel(api_gateway, loyalty_service, "Запросы скидок/статуса", "REST/gRPC")

' Базы данных
Rel_L(user_service, db_user, "Чтение/Запись данных", "SQL/JDBC")
Rel_L(metadata_service, db_meta, "Чтение/Запись данных", "SQL/JDBC")
Rel_L(catalog_service, db_catalog, "Чтение/Запись данных", "SQL/JDBC")

' Внешние интеграции
Rel(user_service, payment_system, "Инициирование/Проверка платежей", "REST/Webhook")
Rel(catalog_service, ext_cinemas, "Сбор/Обновление данных о контенте", "REST/SOAP")

' Асинхронное взаимодействие (Kafka)
Rel(user_service, kafka, "Отправка событий: 'Новый пользователь', 'Оценка'", "Async Publish")
Rel(metadata_service, kafka, "Отправка событий: 'Обновлены метаданные'", "Async Publish")
Rel(catalog_service, kafka, "Отправка событий: 'Новый контент'", "Async Publish")
Rel(loyalty_service, kafka, "Подписка на события пользователя/подписки", "Async Consume")

' Асинхронное взаимодействие (RabbitMQ)
Rel(user_service, rabbitmq, "Отправка запроса на рекомендацию", "Async Publish")
Rel(ext_recommender, rabbitmq, "Публикация рекомендаций", "Async Publish")

' Объекты
Rel(catalog_service, object_storage, "Ссылка на контент/постеры")

@enduml
```