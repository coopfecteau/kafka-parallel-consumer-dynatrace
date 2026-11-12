package demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/** Control group: same work as the parallel consumer, done on the thread that called poll(). */
final class PlainConsumerApp {
    private static final Logger log = LoggerFactory.getLogger(PlainConsumerApp.class);

    static void run() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-plain");
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "orders-plain-consumer");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        OrderWork work = new OrderWork(log, "plain", "orders-plain");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(List.of(Config.TOPIC));
            log.info("plain consumer started: topic={}", Config.TOPIC);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> rec : records) {
                    try {
                        work.process(rec);
                    } catch (Exception e) {
                        log.warn("[plain] failed p{}@{}: {}", rec.partition(), rec.offset(), e.getMessage());
                    }
                }
            }
        }
    }

    private PlainConsumerApp() {}
}
