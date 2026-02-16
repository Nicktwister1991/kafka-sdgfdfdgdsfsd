@echo off
REM ==============================
REM Отправка тестовых данных в Kafka на Windows
REM ==============================

REM --- blocked_users ---
echo Добавляем заблокированных пользователей...
echo user2:["user1"] | docker exec -i kafka-kraft kafka-console-producer.sh --topic blocked_users --bootstrap-server kafka-kraft:9092 --property "parse.key=true" --property "key.separator=:"
echo user3:["user1","user4"] | docker exec -i kafka-kraft kafka-console-producer.sh --topic blocked_users --bootstrap-server kafka-kraft:9092 --property "parse.key=true" --property "key.separator=:"

REM --- banned_words ---
echo Добавляем запрещённые слова...
echo badword:"" | docker exec -i kafka-kraft kafka-console-producer.sh --topic banned_words --bootstrap-server kafka-kraft:9092 --property "parse.key=true" --property "key.separator=:"
echo spam:"" | docker exec -i kafka-kraft kafka-console-producer.sh --topic banned_words --bootstrap-server kafka-kraft:9092 --property "parse.key=true" --property "key.separator=:"

REM --- messages ---
echo Отправляем тестовые сообщения...
echo user1:{"messageId":"101","fromUser":"user1","toUser":"user2","text":"hello user2"} | docker exec -i kafka-kraft kafka-console-producer.sh --topic messages --bootstrap-server kafka-kraft:9092 --property "parse.key=true" --property "key.separator=:"
echo user3:{"messageId":"102","fromUser":"user3","toUser":"user4","text":"this is a badword message"} | docker exec -i kafka-kraft kafka-console-producer.sh --topic messages --bootstrap-server kafka-kraft:9092 --property "parse.key=true" --property "key.separator=:"
echo user4:{"messageId":"103","fromUser":"user4","toUser":"user3","text":"hello everyone"} | docker exec -i kafka-kraft kafka-console-producer.sh --topic messages --bootstrap-server kafka-kraft:9092 --property "parse.key=true" --property "key.separator=:"
echo user1:{"messageId":"104","fromUser":"user1","toUser":"user3","text":"hi there"} | docker exec -i kafka-kraft kafka-console-producer.sh --topic messages --bootstrap-server kafka-kraft:9092 --property "parse.key=true" --property "key.separator=:"

echo ✅ Все тестовые данные отправлены!
pause