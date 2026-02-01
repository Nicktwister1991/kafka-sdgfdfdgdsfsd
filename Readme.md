# KafkaApp

## Описание
Приложение состоит из:
- SimpleProducer — продюсер, отправляющий 10 сообщений в топик kraft-topic-1 с гарантией At Least Once.
- SingleMessageConsumer — читает по одному сообщению, автокоммит.
- BatchMessageConsumer — читает пачки сообщений, коммит после всей пачки.
- KafkaApp — запускает оба консьюмера.
- Kafka и Kafka UI запускаются в Docker, Java-приложение запускается локально.

## Запуск через Docker
1. docker-compose up -d
2. Проверить, что Kafka и Kafka UI поднялись.
3. Запустить продюсер отдельно или включить в KafkaApp.
4. В Kafka UI можно наблюдать топики, оффсеты и lag консьюмеров.

## Проверка работы
- Сообщения должны появляться в топике kraft-topic-1.
- В консоли приложений выводятся сообщения от SingleMessageConsumer и BatchMessageConsumer.
- Если искусственно вызвать ошибку (например, NullPointerException при обработке сообщения), консьюмер пишет ошибку в лог, но продолжает работать.
- Для проверки At Least Once: выключить брокер на короткое время → продюсер повторит отправку, сообщения не потеряются.

## Пояснения параметров
- ProducerConfig.ACKS_CONFIG = "all" — ждать подтверждения от всех реплик (At Least Once)
- ProducerConfig.RETRIES_CONFIG = 3 — повторная отправка при ошибках
- ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG = true/false — авто- или ручной коммит оффсета
- ConsumerConfig.MAX_POLL_RECORDS_CONFIG — сколько сообщений обрабатывается за один poll