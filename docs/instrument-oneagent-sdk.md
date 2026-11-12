# Instrumenting a Confluent Parallel Consumer with the OneAgent SDK

Use this when OneAgent monitors the JVM but the consumer side of a Kafka flow shows up as
disconnected traces, which is what Parallel Consumer (PC) does by default. You create one
**incoming message process** span per record, on the worker thread that processes it, and
link it to the producer with the tag OneAgent already wrote into the message headers.

Reference implementation: `RecordTracing.Sdk` in
[`src/main/java/demo/RecordTracing.java`](../src/main/java/demo/RecordTracing.java).

## Why it's needed

PC polls with one `KafkaConsumer` on its own thread and hands records to a worker pool. The
built-in Kafka consumer sensor attaches the trace context to the polling thread, not to the
worker running your code, so whatever the worker does (HTTP calls, DB calls) starts a new trace.
In our test the built-in sensor didn't produce consumer spans for a plain `KafkaConsumer`
either (`JAVA_KAFKA_LISTENERS` was off on the tenant; see [Untested](#untested)).

## Prerequisites

| Item | Value used here |
|---|---|
| OneAgent | Full-stack mode, 1.351+, installed **before** the JVM starts |
| Java | 17 |
| kafka-clients / PC | 3.9.1 / `io.confluent.parallelconsumer:parallel-consumer-core` 0.5.3.3 |
| Producer | Instrumented by OneAgent as usual (its Kafka sensor writes the trace tag) |

### OneAgent features (Settings → Preferences → OneAgent features, `builtin:oneagent.features`)

These were enabled by default on our tenant. Confirm before you start:

| Key | Why |
|---|---|
| `SENSOR_SDK_JAVA_INCOMING_MESSAGING` | Turns on the SDK's incoming message tracer. If it's off, the tracer does nothing. |
| `KAFKA_TRACE_CONTEXT` | Lets the producer sensor write `X-dynaTrace` / `traceparent` headers. |
| `JAVA_KAFKA` | Producer-side Kafka sensor. |

No other tenant configuration is required.

## Steps

### 1. Add the SDK

```xml
<dependency>
  <groupId>com.dynatrace.oneagent.sdk.java</groupId>
  <artifactId>oneagent-sdk</artifactId>
  <version>1.9.0</version>
</dependency>
```

The jar is a stub. Without OneAgent in the JVM every call is a no-op, so it's safe to ship everywhere.

### 2. Create the SDK and messaging info once

```java
OneAgentSDK sdk = OneAgentSDKFactory.createInstance();
MessagingSystemInfo info = sdk.createMessagingSystemInfo(
        "Kafka", "orders", MessageDestinationType.TOPIC, ChannelType.TCP_IP, "broker:9092");
log.info("OneAgent SDK state={}", sdk.getCurrentState());   // expect ACTIVE
```

If the state is `PERMANENTLY_INACTIVE`, OneAgent isn't injected into this JVM. Restart the
process after OneAgent is installed.

### 3. Wrap the work inside the PC poll function

```java
pc.poll(ctx -> {
    ConsumerRecord<String, String> rec = ctx.getSingleConsumerRecord();
    IncomingMessageProcessTracer t = sdk.traceIncomingMessageProcess(info);

    // OneAgent's Kafka producer sensor writes a BINARY tag named "X-dynaTrace".
    // The SDK constant DYNATRACE_MESSAGE_PROPERTYNAME ("dtdTraceTagInfo") is the
    // string-tag fallback, used by other producers or by the SDK's own outgoing tracer.
    Header byteTag = rec.headers().lastHeader("X-dynaTrace");
    Header strTag  = rec.headers().lastHeader(OneAgentSDK.DYNATRACE_MESSAGE_PROPERTYNAME);
    if (byteTag != null) t.setDynatraceByteTag(byteTag.value());
    else if (strTag != null) t.setDynatraceStringTag(new String(strTag.value(), UTF_8));

    t.setVendorMessageId(rec.partition() + "@" + rec.offset());
    t.setCorrelationId(rec.key());
    t.start();
    try {
        process(rec);              // your business logic; outgoing calls become children
    } catch (Exception e) {
        t.error(e);
        throw e;                   // PC retries the record
    } finally {
        t.end();
    }
});
```

Rules:
- Start and end the tracer **on the worker thread** (inside `poll`), never on the polling thread.
- One tracer per record. For batch mode (`batchSize > 1`), loop over `ctx.getConsumerRecordsFlattened()`
  and trace each record separately; one tracer can carry only one parent tag.
- Always `end()` in `finally`, or the span never closes.

### 4. (Optional) Tag the process

```powershell
$env:DT_TAGS = "primary_tags.instrumentation=sdk"   # read by OneAgent at process start
```

## Verify

Allow 2–3 minutes for ingest, then:

```
fetch spans, from:now()-10m
| filter span.kind == "consumer" and code.namespace == "OneAgent SDK"
| summarize spans=count(), linked=countIf(isNotNull(span.parent_id)), by:{dt.service.name, endpoint.name}
```

Expected: `dt.service.name = Kafka Messaging`, `endpoint.name = Topic: orders`, and `linked == spans`.
End to end, one trace should contain the producer's `orders publish` span, then `orders process`,
then any calls the worker makes.

## What you get (observed)

| Field | Value |
|---|---|
| Service / endpoint | `Kafka Messaging` / `Topic: orders` (Dynatrace messaging service) |
| `transaction.is_message_processing` | `true` |
| `messaging.system`, `.destination.name`, `.operation.type` | `kafka`, `orders`, `process` |
| `messaging.message.id` / `.conversation_id` | from `setVendorMessageId` / `setCorrelationId` |
| `server.address` / `server.port` | broker endpoint from `MessagingSystemInfo` |
| Failures | `request.is_failed` plus an exception span event |

Not available through the SDK API: consumer group, partition, offset, or message key as
separate attributes. Encode them into the message id / correlation id if you need them.

## Untested

`JAVA_KAFKA_LISTENERS` (built-in Kafka consumer listener sensor) was **off** on our tenant.
Turning it on may produce native consumer spans for a plain `KafkaConsumer`. It's unlikely to
fix PC's worker hand-off, and running it alongside the SDK would create duplicate consumer spans.
