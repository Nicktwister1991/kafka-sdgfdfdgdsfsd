package org.example.kafka; // расположение класса

import org.apache.kafka.clients.producer.*; // импортируем  классы для работы продюсера
import org.apache.kafka.common.serialization.StringSerializer;// класс для преобразования ключей и сообщения в байты

import java.util.Properties; // стандартный класс джавы для хранения настроек в виде ключ-значения

public class SimpleProducer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9094,localhost:9095"; // список серверов кафки
    private static final String TOPIC = "kraft-topic-1"; // или zk-topic-1

    public static void main(String[] args) {

        //  Настройки продюсера
        Properties props = new Properties(); // создаем объект в котором будем хранить настройки продюсера
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);//передаем сервера для подключения
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //сериализуем ключ в строку
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());//сериализуем сообщения в строку

        // Гарантия доставки

        props.put(ProducerConfig.ACKS_CONFIG, "all"); // ждем подтверждения от всех реплик
        props.put(ProducerConfig.RETRIES_CONFIG, 3);  // повторная отправка при ошибках
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5); // немного буферизуем, чтобы не отправлять каждое сообщение сразу
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false); // отключаем идемпотентность для At Least Once

        // 2. Создаём продюсер
        KafkaProducer<String, String> producer = new KafkaProducer<>(props); //создаем продюсера

        try {
            // Отправка сообщений (push)
            for (int i = 1; i <= 10; i++) {
                String key = String.valueOf(i);
                String value = "Сообщение №" + i;

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC, key, value); //создаем единичное сообщение, в котором указываем топик,ключ и значение

                // Асинхронная отправка
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Ошибка отправки: " + exception.getMessage());
                    } else {
                        System.out.printf(
                                "Отправлено: topic=%s partition=%d offset=%d%n",
                                metadata.topic(),
                                metadata.partition(),
                                metadata.offset()
                        );
                    } // если ошибка, иначе выводим на экран тему, партицию и офсет
                });
            }

        } finally {
            // 4. Работа с IO-потоком
            producer.flush(); // дождаться отправки всех сообщений
            producer.close(); // закрываем продюсер что бы не оставлять ненужных открытых потоков
        }

    }
}