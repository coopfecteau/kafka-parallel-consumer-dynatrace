# Parallel Consumer × OneAgent test bed

Tests how well OneAgent traces a Kafka producer and a Confluent Parallel Consumer
(`io.confluent.parallelconsumer:parallel-consumer-core` 0.5.3.3, kafka-clients 3.9.1).

| Role | Process | What it does |
|---|---|---|
| `producer` | `java -jar target/pc-oneagent-demo.jar producer` | `POST :8080/order` → `KafkaProducer` → topic `orders` (6 partitions, key = customer). A built-in load generator calls `/order` at `RATE`/s. Also serves `GET :8081/inventory`. |
| `parallel-consumer` | `... parallel-consumer` | PC, `ProcessingOrder.KEY`, 16 workers; each record → `GET /inventory` + sleep; 3% simulated failures (PC retries them). |
| `plain-consumer` | `... plain-consumer` | Control group: plain `KafkaConsumer` poll loop doing the same work on the poll thread. |

Env overrides: `BOOTSTRAP_SERVERS`, `TOPIC`, `RATE`, `PC_CONCURRENCY`, `ORDER_PORT`, `INVENTORY_PORT`.

## Instrumentation guides

- [OneAgent SDK](docs/instrument-oneagent-sdk.md): incoming message tracer, linked via the `X-dynaTrace` tag
- [OpenTelemetry API captured by OneAgent](docs/instrument-otel.md): no OTel SDK or agent

## Local run (Windows, no Docker)

```powershell
.\scripts\setup-local.ps1               # one time: Kafka 4.4.0 + Maven into .\tools, patched for Windows
$env:KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"   # skips the wmic call missing on Win11 24H2+
tools\kafka\bin\windows\kafka-server-start.bat tools\kafka\config\local.properties

tools\apache-maven-3.9.16\bin\mvn.cmd -q package -DskipTests
java -jar target\pc-oneagent-demo.jar producer
java -jar target\pc-oneagent-demo.jar parallel-consumer
java -jar target\pc-oneagent-demo.jar plain-consumer
```

`tools/kafka/bin/windows/kafka-run-class.bat` is patched to use `libs\*` as the classpath
(the stock per-jar loop overflows cmd's line limit). Start the JVMs **after** OneAgent is
installed; it only instruments processes launched afterwards.

## Experiments

`.\run.ps1 -Mode <none|sdk|otel>` restarts the three JVMs with `TRACING=<mode>` and
`DT_TAGS=primary_tags.instrumentation=<mode>` (OneAgent reads `DT_TAGS` at process start).
The producer side is always native OneAgent; the mode only changes the consumer side
(`src/main/java/demo/RecordTracing.java`), and applies to both consumers.

| Mode | Consumer span from | Linked to producer via | Result on hsn (Nov 2026) |
|---|---|---|---|
| `none` | OneAgent Kafka sensor | — | No consumer spans at all, PC or plain. `JAVA_KAFKA_LISTENERS` is **off** in `builtin:oneagent.features`; untested whether turning it on fixes this. |
| `sdk` | OneAgent SDK `traceIncomingMessageProcess` | `X-dynaTrace` byte tag | Fully linked; service "Kafka Messaging". |
| `otel` | OTel **API** only (no SDK, no OTel agent); OneAgent's `JAVA_OPENTELEMETRY` sensor backs `GlobalOpenTelemetry` | OneAgent's registered propagator + case-insensitive header getter | Fully linked, no trace flaws; not a named messaging service. |

OTel gotcha: extracting with `W3CTraceContextPropagator.getInstance()` produces a valid-looking
parent that OneAgent silently ignores (every consumer span becomes a root). Extract with
`GlobalOpenTelemetry.getPropagators().getTextMapPropagator()`.

Neither experiment needs tenant configuration changes. Both rely only on OneAgent feature
defaults (`SENSOR_SDK_JAVA_INCOMING_MESSAGING`, `JAVA_OPENTELEMETRY`, `KAFKA_TRACE_CONTEXT`).

## Checking it in Dynatrace

```
fetch spans, from:now()-15m
| filter java.jar.file == "pc-oneagent-demo.jar"
| summarize spans=count(), roots=countIf(isNull(span.parent_id)),
    by:{pid=toString(process.pid), span.kind, span.name, code.namespace}
```

A linked consumer shows `consumer` spans and its `GET` client spans have a parent. A
`GET` with `roots == spans` means the consumer-side work started a new trace.
