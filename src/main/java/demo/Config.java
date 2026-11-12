package demo;

final class Config {
    static final String BOOTSTRAP = env("BOOTSTRAP_SERVERS", "localhost:9092");
    static final String TOPIC = env("TOPIC", "orders");
    static final int ORDER_PORT = Integer.parseInt(env("ORDER_PORT", "8080"));
    static final int INVENTORY_PORT = Integer.parseInt(env("INVENTORY_PORT", "8081"));
    /** Orders per second the producer's built-in load generator sends to its own /order endpoint. */
    static final double RATE = Double.parseDouble(env("RATE", "5"));
    static final int PC_CONCURRENCY = Integer.parseInt(env("PC_CONCURRENCY", "16"));

    static String inventoryUrl(String sku) {
        return "http://localhost:" + INVENTORY_PORT + "/inventory?sku=" + sku;
    }

    private static String env(String k, String def) {
        String v = System.getenv(k);
        return v == null || v.isBlank() ? def : v;
    }

    private Config() {}
}
