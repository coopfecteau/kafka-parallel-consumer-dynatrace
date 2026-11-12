# Instrumenting a Confluent Parallel Consumer with OpenTelemetry (captured by OneAgent)

Use this when you want vendor-neutral instrumentation code but OneAgent is the collector.
You add **only the OpenTelemetry API** to the app, with no OTel SDK, exporter, or OTel Java agent.
OneAgent's OpenTelemetry sensor replaces the no-op `GlobalOpenTelemetry` with its own tracer
and ships the spans with the rest of the trace.

Reference implementation: `RecordTracing.Otel` in
[`src/main/java/demo/RecordTracing.java`](../src/main/java/demo/RecordTracing.java).

## Why it's needed

Parallel Consumer (PC) polls on one thread and processes on a worker pool, so the built-in
Kafka consumer sensor's trace context never reaches your code. You create a `CONSUMER` span
per record on the worker thread, parented on the context the producer wrote into the headers.

## Prerequisites

| Item | Value used here |
|---|---|
| OneAgent | Full-stack mode, 1.351+, installed **before** the JVM starts |
| Java | 17 |
| kafka-clients / PC | 3.9.1 / 0.5.3.3 |
| Producer | Instrumented by OneAgent (writes `X-dynaTrace`, `traceparent`, `tracestate`) |

### OneAgent features (`builtin:oneagent.features`)

| Key | Setting | Why |
|---|---|---|
| `JAVA_OPENTELEMETRY` | **on** | OneAgent backs `GlobalOpenTelemetry` and captures API spans. |
| `KAFKA_TRACE_CONTEXT` | on | Producer writes trace headers. |
| `JAVA_OPENTELEMETRY_JAVA_INSTRUMENTATION_AGENT` | leave **off** | Only for the OTel Java agent, which isn't used here. |

### Span settings

| Schema | What we did |
|---|---|
| `builtin:span-entry-points` | Nothing. The defaults suppress only CLIENT/INTERNAL/PRODUCER entry points, so CONSUMER spans become entry points. |
| `builtin:span-capturing` | Nothing. The default captures all spans. |
| `builtin:span-context-propagation` | Nothing. A PROPAGATE rule is **not** needed: with no rules, linking still worked and was flag-free, verified on hsn. The extraction in steps 2–3 is what makes it work. |

No tenant configuration changes are required beyond the defaults above.

## Steps

### 1. Add the OTel API (API only)

```xml
<dependency>
  <groupId>io.opentelemetry</groupId>
  <artifactId>opentelemetry-api</artifactId>
  <version>1.68.0</version>
</dependency>
```

Don't add `opentelemetry-sdk`, an exporter, or `-javaagent:opentelemetry-javaagent.jar`. An OTel SDK
registering itself as global would take over from OneAgent.

### 2. Get the tracer and OneAgent's propagator once

```java
Tracer tracer = GlobalOpenTelemetry.getTracer("demo.kafka-consumer", "1.0.0");
TextMapPropagator propagator = GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
log.info("tracer={} propagatorFields={}", tracer.getClass().getName(), propagator.fields());
```

Under OneAgent this logs `com.dynatrace.agent.introspection.span.opentelemetry.DynatraceTracer` and
fields `[baggage, x-dynatrace, traceparent, tracestate]`. A `DefaultTracer` or empty field list
means OneAgent isn't injected (or `JAVA_OPENTELEMETRY` is off), and every span is silently dropped.

> **Gotcha (cost us a run):** extracting with `W3CTraceContextPropagator.getInstance()` returns a
> valid-looking remote parent, but OneAgent ignores it and every consumer span becomes a root.
> There's no error. Always extract with `GlobalOpenTelemetry.getPropagators()`.

### 3. A header getter for Kafka (case-insensitive, binary-safe)

```java
static final TextMapGetter<Headers> HEADERS = new TextMapGetter<>() {
    public Iterable<String> keys(Headers h) {
        List<String> k = new ArrayList<>(); h.forEach(x -> k.add(x.key())); return k;
    }
    public String get(Headers h, String key) {
        if (h == null) return null;
        Header found = null;
        for (Header x : h) if (x.key().equalsIgnoreCase(key)) found = x;   // last wins
        if (found == null || found.value() == null) return null;
        return new String(found.value(), key.equalsIgnoreCase("x-dynatrace")
                ? StandardCharsets.ISO_8859_1 : StandardCharsets.UTF_8);
    }
};
```

Both details matter:
- **Case-insensitive:** OneAgent's propagator asks for `x-dynatrace`, but OneAgent's Kafka producer
  sensor writes `X-dynaTrace`, and `Headers.lastHeader` is exact-match. The naive
  `h.lastHeader(key)` never finds it, so the link falls back to W3C only and every consumer span
  is flagged `supportability.flaws = [C2, S11]` (see [Trace flaws](#trace-flaws-c2-s11-resolved)).
- **ISO-8859-1 for the Dynatrace tag:** the value is binary. ISO-8859-1 maps bytes to chars 1:1;
  UTF-8 would corrupt it.

### 4. Wrap the work inside the PC poll function

```java
pc.poll(ctx -> {
    ConsumerRecord<String, String> rec = ctx.getSingleConsumerRecord();
    Context parent = propagator.extract(Context.root(), rec.headers(), HEADERS);
    Span span = tracer.spanBuilder(rec.topic() + " process")
            .setParent(parent)
            .setSpanKind(SpanKind.CONSUMER)
            .setAttribute("messaging.system", "kafka")
            .setAttribute("messaging.operation.type", "process")
            .setAttribute("messaging.destination.name", rec.topic())
            .setAttribute("messaging.consumer.group.name", "orders-parallel")
            .setAttribute("messaging.destination.partition.id", String.valueOf(rec.partition()))
            .setAttribute(AttributeKey.longKey("messaging.kafka.offset"), rec.offset())
            .setAttribute("messaging.kafka.message.key", rec.key())
            .startSpan();
    try (Scope ignored = span.makeCurrent()) {   // outgoing calls OneAgent sees become children
        process(rec);
    } catch (Exception e) {
        span.recordException(e);
        span.setStatus(StatusCode.ERROR, e.getMessage());
        throw e;                                  // PC retries the record
    } finally {
        span.end();
    }
});
```

Rules:
- Create the span and `makeCurrent()` **on the worker thread**, inside `poll`.
- Set attributes at span start where you can; OneAgent records them as start attributes.
- In batch mode, create one span per record, or one batch span with span links to each record's context.

### 5. (Optional) Tag the process

```powershell
$env:DT_TAGS = "primary_tags.instrumentation=otel"
```

## Verify

```
fetch spans, from:now()-10m
| filter span.kind == "consumer" and otel.scope.name == "demo.kafka-consumer"
| summarize spans=count(), linked=countIf(isNotNull(span.parent_id)), by:{dt.service.name, endpoint.name}
```

Expected: `linked == spans`. If `linked == 0`, recheck step 2 (propagator) before anything else.

## What you get (observed)

| Field | Value |
|---|---|
| Service / endpoint | `Requests executed in background threads of <jar>` with the step 3 getter; `Span default service` when linked W3C-only. Either way, not a named messaging service. |
| Messaging attributes | Everything you set: consumer group, partition, offset, key, plus `messaging.*` |
| `otel.scope.name` / `.version`, `span.tracing.system` | `demo.kafka-consumer` / `1.0.0`, `opentelemetry` |
| Failures | `request.is_failed`, `span.status_code = error`, exception span event |
| Not present (vs SDK) | `transaction.is_message_processing`, broker `server.address`/`port`, Dynatrace messaging service |
| `supportability.flaws` | None with the step 3 getter. See [Trace flaws](#trace-flaws-c2-s11-resolved). |

To get a named service rather than `Span default service`, add a service-detection rule
for these spans (for example on `otel.scope.name` or `messaging.destination.name`). Not tested yet.

## Trace flaws (C2, S11): resolved

| Getter | Consumer spans linked | With `supportability.flaws = [C2, S11]` |
|---|---|---|
| Exact-match `lastHeader(key)` (W3C-only link) | 3310 / 3310 | 3310 / 3310 |
| Case-insensitive, ISO-8859-1 for `x-dynatrace` (step 3) | 337 / 337 | **0 / 337** |
| OneAgent SDK, for comparison | 1499 / 1499 | 0 / 1499 |

The flaw sat only on the consumer span that joins the remote parent; `trace.id` and
`span.parent_id` were correct either way. Dynatrace doesn't publicly document C2/S11. What we
observed is that linking with only `traceparent`/`tracestate` is flagged, and handing OneAgent's
propagator its native `X-dynaTrace` tag clears it. A small number of `GET /inventory` server
spans (~1%) carry the same codes in every mode, clustered when a consumer was killed mid-request.
That looks like genuinely lost caller data and is unrelated.

## Portability note

Your code is plain OTel, but with only the API on the classpath, spans exist only when OneAgent
is present. To send them elsewhere, add the OTel SDK and an exporter, and stop relying on OneAgent's capture.
