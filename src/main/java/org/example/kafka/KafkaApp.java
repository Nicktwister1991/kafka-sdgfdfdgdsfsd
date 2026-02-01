package org.example.kafka;

public class KafkaApp {
    public static void main(String[] args) {
        Thread consumer1 = new Thread(new SingleMessageConsumer("group-single", "kraft-topic-1"));
        Thread consumer2 = new Thread(new BatchMessageConsumer("group-batch", "kraft-topic-1"));

        consumer1.start();
        consumer2.start();

        // Продюсер можно запускать отдельно или тут
        // SimpleProducer.main(args);
    }
}