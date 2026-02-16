package org.example.kafka;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Properties;

public class MessagingStreamApp {

    public static void main(String[] args) {

        // ============================
        // 1️⃣ Конфигурация Kafka Streams
        // ============================
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "messaging-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094"); // KRaft брокер
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // ============================
        // 2️⃣ Поток сообщений из топика messages
        // ============================
        KStream<String, String> messages = builder.stream("messages");

        ObjectMapper objectMapper = new ObjectMapper();

        // ============================
        // 3️⃣ Обработка: фильтрация и цензура
        // ============================
        KStream<String, String> filteredMessages = messages.mapValues(value -> {
                    try {
                        ObjectNode node = (ObjectNode) objectMapper.readTree(value);
                        String fromUser = node.get("fromUser").asText();
                        String toUser = node.get("toUser").asText();
                        String text = node.get("text").asText();

                        // ----------------------------
                        // 3a️⃣ Фильтрация заблокированных пользователей
                        // ----------------------------
                        // Пример: user1 заблокирован у user2
                        if (toUser.equals("user2") && fromUser.equals("user1")) {
                            return null; // сообщение не проходит
                        }

                        // ----------------------------
                        // 3b️⃣ Цензура запрещённых слов
                        // ----------------------------
                        String[] bannedWords = {"badword", "spam"}; // пример запрещённых слов
                        for (String word : bannedWords) {
                            text = text.replaceAll("(?i)\\b" + word + "\\b", "***");
                        }

                        node.put("text", text);
                        return objectMapper.writeValueAsString(node);

                    } catch (Exception e) {
                        e.printStackTrace();
                        return value; // если ошибка → возвращаем оригинальное сообщение
                    }
                })
                .filter((key, value) -> value != null); // удаляем заблокированные сообщения

        // ============================
        // 4️⃣ Вывод в консоль
        // ============================
        filteredMessages.foreach((key, value) -> System.out.println("Filtered message: " + value));

        // ============================
        // 5️⃣ Отправка в топик filtered_messages
        // ============================
        filteredMessages.to("filtered_messages");

        // ============================
        // 6️⃣ Запуск Kafka Streams
        // ============================
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Корректное закрытие при остановке приложения
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}