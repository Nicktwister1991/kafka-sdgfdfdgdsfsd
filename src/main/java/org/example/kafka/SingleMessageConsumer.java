package org.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration; // для указания времени ожидания забора данных из кафки
import java.util.Collections; // для создания списка тем
import java.util.Properties; // хранение настроек консюмера

// Создаем консюмера с налседованием класса Runnable - это нужно что бы консюмеры работали каждый в своем потоке а не по очереди
public class SingleMessageConsumer implements Runnable  {

    private final String groupId; // название группы консюмера
    private final String topic; // имя темы куда подключаться

    public SingleMessageConsumer(String groupId, String topic) {
        this.groupId = groupId; // когда запускаем консюмера, мы передаем ему его группу
        this.topic = topic; //когда запускаем консюмера, мы передаем ему его топик
    }
    // метод запускает консюмера
    @Override
    public void run() {
        Properties props = new Properties(); // создаем конфигурации
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094,localhost:9095");// указываем брокеров для подключения
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);//указываем группу
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // преобразуем байти из кафки в строку. Ключ
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // преобразуем байти из кафки в строку. Содержимое сообщений
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // авто-коммит после каждого сообщения
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // читать с самого начала партиции
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