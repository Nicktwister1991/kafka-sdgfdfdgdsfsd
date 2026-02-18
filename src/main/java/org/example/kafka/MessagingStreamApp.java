
package org.example.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.tools.javac.Main;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.io.InputStream;
import java.util.*;

public class MessagingStreamApp {

    public static void main(String[] args) throws Exception {

        // ===== Загружаем конфигурацию =====
        Properties appProps = new Properties();
        InputStream input = Main.class.getClassLoader()
                .getResourceAsStream("application.properties");
        appProps.load(input);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                appProps.getProperty("application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                appProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper();

        // ===== Локальные структуры (динамически обновляются) =====
        Map<String, Set<String>> blockedUsersMap = new HashMap<>();
        Set<String> bannedWordsSet = new HashSet<>();

        // ===== KTable blocked_users =====
        KTable<String, String> blockedUsersTable =
                builder.table(appProps.getProperty("topic.blocked"));

        blockedUsersTable.toStream().foreach((recipient, blockedList) -> {
            if (blockedList == null) return;

            Set<String> blocked =
                    new HashSet<>(Arrays.asList(blockedList.split(",")));

            blockedUsersMap.put(recipient, blocked);

            System.out.println("Updated blocked list for " + recipient);
        });

        // ===== KTable banned_words =====
        KTable<String, String> bannedWordsTable =
                builder.table(appProps.getProperty("topic.banned"));

        bannedWordsTable.toStream().foreach((key, word) -> {
            if (word != null) {
                bannedWordsSet.add(word);
                System.out.println("Added banned word: " + word);
            }
        });

        // ===== Поток сообщений =====
        KStream<String, String> messages =
                builder.stream(appProps.getProperty("topic.messages"));

        KStream<String, String> processed =
                messages.mapValues(value -> {

                            try {
                                // Десериализация JSON в объект Message
                                Message msg =
                                        mapper.readValue(value, Message.class);

                                // ===== Проверка блокировки =====
                                Set<String> blocked =
                                        blockedUsersMap.get(msg.getRecipientId());

                                if (blocked != null &&
                                        blocked.contains(msg.getUserId())) {

                                    System.out.println(
                                            "Message blocked from "
                                                    + msg.getUserId()
                                                    + " to "
                                                    + msg.getRecipientId());

                                    return null;
                                }

                                // ===== Цензура =====
                                String updated = msg.getMessage();

                                for (String banned : bannedWordsSet) {
                                    updated = updated.replaceAll(banned, "***");
                                }

                                msg.setMessage(updated);

                                System.out.println("Processed message: "
                                        + updated);


                                return mapper.writeValueAsString(msg);

                            } catch (Exception e) {
                                e.printStackTrace();
                                return null;
                            }
                        })
                        .filter((k, v) -> v != null); // отбрасываем заблокированные

        // ===== Отправка в топик filtered =====
        processed.to(appProps.getProperty("topic.filtered"));

        // ===== Запуск потоков =====
        KafkaStreams streams =
                new KafkaStreams(builder.build(), props);

        streams.start();

        Runtime.getRuntime().addShutdownHook(
                new Thread(streams::close));
    }
}
