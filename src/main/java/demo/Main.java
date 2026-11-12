package demo;

/**
 * One jar, three processes. Each runs as its own JVM so OneAgent sees a separate
 * process group per role:
 *
 *   producer          HTTP /order -> KafkaProducer -> topic "orders"; also hosts /inventory
 *   parallel-consumer Confluent Parallel Consumer (key-ordered, 16 workers) -> calls /inventory
 *   plain-consumer    Plain KafkaConsumer poll loop doing the same work, as a control group
 *
 * The plain consumer exists so a trace gap can be attributed: if it links to the producer's
 * trace but the parallel consumer does not, the gap is PC's thread hand-off, not Kafka tracing.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        String role = args.length > 0 ? args[0] : "";
        switch (role) {
            case "producer" -> ProducerApp.run();
            case "parallel-consumer" -> ParallelConsumerApp.run();
            case "plain-consumer" -> PlainConsumerApp.run();
            default -> {
                System.err.println("usage: java -jar pc-oneagent-demo.jar producer|parallel-consumer|plain-consumer");
                System.exit(2);
            }
        }
    }
}
