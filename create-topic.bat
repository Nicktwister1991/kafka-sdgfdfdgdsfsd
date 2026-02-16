@echo off
REM ==============================
REM Создание топиков на KRaft и ZooKeeper
REM ==============================

REM --- KRaft кластер (старые топики) ---
echo.
echo Создаём существующие топики на KRaft кластере...
docker exec -i kafka-kraft kafka-topics.sh --create --if-not-exists --topic kraft-topic-1 --partitions 3 --replication-factor 2 --bootstrap-server kafka-kraft:9092
docker exec -i kafka-kraft kafka-topics.sh --create --if-not-exists --topic kraft-topic-2 --partitions 3 --replication-factor 2 --bootstrap-server kafka-kraft:9092


REM ==============================
REM 🔥 Топики для Практической работы 2
REM ==============================

echo.
echo Создаём топики для сервиса обмена сообщениями...

docker exec -i kafka-kraft kafka-topics.sh --create --if-not-exists --topic messages --partitions 3 --replication-factor 2 --bootstrap-server kafka-kraft:9092

docker exec -i kafka-kraft kafka-topics.sh --create --if-not-exists --topic filtered_messages --partitions 3 --replication-factor 2 --bootstrap-server kafka-kraft:9092

docker exec -i kafka-kraft kafka-topics.sh --create --if-not-exists --topic blocked_users --partitions 3 --replication-factor 2 --bootstrap-server kafka-kraft:9092

docker exec -i kafka-kraft kafka-topics.sh --create --if-not-exists --topic banned_words --partitions 3 --replication-factor 2 --bootstrap-server kafka-kraft:9092


REM --- ZooKeeper кластер (старые топики) ---
echo.
echo Ждём 10 секунд, пока оба брокера ZooKeeper стартуют...
timeout /t 10 /nobreak >nul

echo Создаём топики на ZooKeeper кластере...
docker exec -i kafka-zk /usr/bin/kafka-topics --create --if-not-exists --topic zk-topic-1 --partitions 3 --replication-factor 2 --bootstrap-server kafka-zk:9092
docker exec -i kafka-zk /usr/bin/kafka-topics --create --if-not-exists --topic zk-topic-2 --partitions 3 --replication-factor 2 --bootstrap-server kafka-zk:9092


REM --- Проверка топиков ---
echo.
echo Список топиков на KRaft:
docker exec -i kafka-kraft kafka-topics.sh --list --bootstrap-server kafka-kraft:9092

echo.
echo Список топиков на ZooKeeper:
docker exec -i kafka-zk /usr/bin/kafka-topics --list --bootstrap-server kafka-zk:9092

echo.
echo ✅ Все топики успешно созданы!
pause