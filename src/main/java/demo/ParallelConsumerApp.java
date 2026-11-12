package demo;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * Confluent Parallel Consumer: one KafkaConsumer polls on PC's broker-poll thread, and records
 * are handed to a worker pool. That hand-off is what OneAgent has to follow; the Kafka
 * sensor hooks poll()/record iteration, which happens on a different thread from the work.
 */
final class ParallelConsumerApp {
    private static final Logger log = LoggerFactory.getLogger(ParallelConsumerApp.class);

    static void run() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-parallel");
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "orders-parallel-consumer");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // PC owns commits; it refuses to start if the consumer auto-commits.
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        ParallelConsumerOptions<String, String> options = ParallelConsumerOptions.<String, String>builder()
                .consumer(new KafkaConsumer<>(p))
                .ordering(ProcessingOrder.KEY)
                .maxConcurrency(Config.PC_CONCURRENCY)
                .commitMode(CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS)
                .build();

        ParallelStreamProcessor<String, String> pc = ParallelStreamProcessor.createEosStreamProcessor(options);
        pc.subscribe(List.of(Config.TOPIC));
        Runtime.getRuntime().addShutdownHook(new Thread(pc::close));

        OrderWork work = new OrderWork(log, "parallel", "orders-parallel");
        log.info("parallel consumer started: topic={} ordering=KEY concurrency={}", Config.TOPIC, Config.PC_CONCURRENCY);
        // A thrown exception makes PC retry the record with backoff, which shows up as
        // repeated attempts on the same offset rather than a skipped message.
        pc.poll(ctx -> {
            try {
                work.process(ctx.getSingleConsumerRecord());
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private ParallelConsumerApp() {}
}
