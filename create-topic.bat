@echo off
REM ==============================
REM Создание топиков на KRaft и ZooKeeper
REM ==============================

REM --- KRaft кластер ---
echo.
echo Создаём топики на KRaft кластере...
docker exec -i kafka-kraft kafka-topics.sh --create --topic kraft-topic-1 --partitions 3 --replication-factor 2 --bootstrap-server kafka-kraft:9092
docker exec -i kafka-kraft kafka-topics.sh --create --topic kraft-topic-2 --partitions 3 --replication-factor 2 --bootstrap-server kafka-kraft:9092

REM --- ZooKeeper кластер ---
echo.
echo Ждём 10 секунд, пока оба брокера ZooKeeper стартуют...
timeout /t 10 /nobreak >nul

echo Создаём топики на ZooKeeper кластере...
docker exec -i kafka-zk /usr/bin/kafka-topics --create --topic zk-topic-1 --partitions 3 --replication-factor 1 --bootstrap-server kafka-zk:9092
docker exec -i kafka-zk /usr/bin/kafka-topics --create --topic zk-topic-2 --partitions 3 --replication-factor 1 --bootstrap-server kafka-zk:9092

REM --- Проверка топиков ---
echo.
echo Список топиков на KRaft:
docker exec -i kafka-kraft kafka-topics.sh --list --bootstrap-server kafka-kraft:9092

echo.
echo Список топиков на ZooKeeper:
docker exec -i kafka-zk /usr/bin/kafka-topics --list --bootstrap-server kafka-zk:9092

echo.
echo ✅ Топики успешно созданы!
pause