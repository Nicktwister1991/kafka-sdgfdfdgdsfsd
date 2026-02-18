# MessagingStreamApp

Приложение на Java с Kafka Streams для фильтрации и цензуры сообщений.  
Система реализует упрощённый сервис обмена сообщениями с возможностью блокировки пользователей и маскировкой запрещённых слов.

---

## 🔹 Цель проекта

- Закрепить знания о потоковой обработке данных и архитектуре распределённых систем на примере Kafka.
- Реализовать обработку сообщений с фильтрацией и цензурой в реальном времени.

---

## 🔹 Структура проекта

- MessagingStreamApp.java — основной код приложения на Java.
- send_test_data.bat — скрипт для отправки тестовых данных в Kafka (Windows).
- docker-compose.yml — развёртывание Kafka KRaft и Kafka UI.
- Readme.md — инструкция по запуску и тестированию проекта.

---

## 🔹 Топики Kafka

### KRaft кластер

- messages — входящие сообщения.
- filtered_messages — сообщения после фильтрации и цензуры.
- blocked_users — данные о заблокированных пользователях.
- banned_words — список запрещённых слов.

---

## 🔹 Формат сообщений

### 1️⃣ blocked_users

| Key    | Value                  |
|--------|-----------------------|
| user2  | ["user1"]             |
| user3  | ["user1","user4"]     |

### 2️⃣ banned_words

| Key      | Value |
|----------|-------|
| badword  | ""    |
| spam     | ""    |

### 3️⃣ messages

| Key    | Value (JSON)                                             |
|--------|----------------------------------------------------------|
| user1  | {"user_id":"user1","recipient_id":"user2","message":"hello user2"} |
| user3  | {"user_id":"user3","recipient_id":"user4","message":"this is a badword message"} |
| user4  | {"user_id":"user4","recipient_id":"user3","message":"hello everyone"} |
| user1  | {"user_id":"user1","recipient_id":"user3","message":"hi there"} |



---

## 🔹 Запуск проекта

1. Убедитесь, что Docker и Docker Compose установлены docker-compose up -d.
2. Запустите кластер Kafka KRaft и Kafka UI:



# Тестирование
## Отправка тестовых сообщений
Используйте скрипт send_test_data.bat (Windows) или send_test_data.sh (Linux/macOS), который публикует тестовые сообщения в топик messages.
Пример тестовых сообщений:

{"user_id":"user1","recipient_id":"user2","message":"hello user2"}
{"user_id":"user3","recipient_id":"user4","message":"this is a badword message"}
## Проверка работы блокировки
Если пользователь user1 заблокирован у user2, сообщение не попадёт в filtered_messages.
Логи приложения покажут:

Message blocked from user1 to user2
## Проверка цензуры
Сообщения, содержащие запрещённые слова, будут маскироваться:

## Processed message: this is a *** message
Сообщение будет отправлено в топик filtered_messages в виде JSON.
4️⃣ Просмотр результатов
Через Kafka UI можно проверить содержимое топика filtered_messages и убедиться, что:
Заблокированные сообщения отсутствуют.
Запрещённые слова замаскированы.

## Динамическое обновление данных
blocked_users — KTable, автоматически обновляет локальный список заблокированных пользователей.
banned_words — KTable, автоматически обновляет набор запрещённых слов.
Изменения вступают в силу в реальном времени без перезапуска приложения.


