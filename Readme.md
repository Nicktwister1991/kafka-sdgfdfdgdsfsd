# MessagingStreamApp

Приложение на Java с Kafka Streams для фильтрации и цензуры сообщений.  
Система реализует упрощённый сервис обмена сообщениями с возможностью блокировки пользователей и маскировкой запрещённых слов.

---

## 🔹 Цель проекта

- Закрепить знания о потоковой обработке данных и архитектуре распределённых систем на примере Kafka.
- Реализовать обработку сообщений с фильтрацией и цензурой.

---

## 🔹 Структура проекта

- MessagingStreamApp.java — основной код приложения на Java.
- send_test_data.bat — скрипт для отправки тестовых данных в Kafka (для Windows).
- docker-compose.yml — развёртывание Kafka KRaft и Kafka UI.
- README.md — инструкция по запуску и тестированию проекта.

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
| user1  | {"messageId":"101","fromUser":"user1","toUser":"user2","text":"hello user2"} |
| user3  | {"messageId":"102","fromUser":"user3","toUser":"user4","text":"this is a badword message"} |
| user4  | {"messageId":"103","fromUser":"user4","toUser":"user3","text":"hello everyone"} |
| user1  | {"messageId":"104","fromUser":"user1","toUser":"user3","text":"hi there"} |

---

## 🔹 Запуск проекта

1. Убедитесь, что Docker и Docker Compose установлены.
2. Запустите кластер Kafka KRaft и Kafka UI:

```bash
docker-compose up -d