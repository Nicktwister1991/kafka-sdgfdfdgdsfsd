package org.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SingleMessageConsumer implements Runnable {

    private final String groupId;
    private final String topic;

    public SingleMessageConsumer(String groupId, String topic) {
        this.groupId = groupId;
        this.topic = topic;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // авто-коммит после каждого сообщения
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"); // по одному сообщению за poll

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            // Обработка одного сообщения
                            System.out.printf("[Single] key=%s value=%s partition=%d offset=%d%n",
                                    record.key(), record.value(), record.partition(), record.offset());
                        } catch (Exception e) {
                            // Логируем ошибку обработки конкретного сообщения и продолжаем
                            System.err.println("[Single] Ошибка при обработке сообщения: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                } catch (Exception e) {
                    // Логируем ошибку polling и продолжаем
                    System.err.println("[Single] Ошибка при poll сообщений: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            // Логируем ошибки создания консьюмера или других критичных проблем
            System.err.println("[Single] Критическая ошибка консьюмера: " + e.getMessage());
            e.printStackTrace();
        }
    }
}