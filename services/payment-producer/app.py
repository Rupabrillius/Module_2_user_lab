import os
import random

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes


from common.kafka import make_producer, safe_sleep
from common.event import envelope
from common.metrics import start_metrics, events_produced_total

TOPIC = "smartdine.payments"

def setup_tracing():
    service_name = os.environ.get("SERVICE_NAME", "unknown-service")

    resource = Resource.create({
        ResourceAttributes.SERVICE_NAME: service_name
    })

    provider = TracerProvider(resource=resource)

    processor = BatchSpanProcessor(
        OTLPSpanExporter(endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"))
    )
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)

    return trace.get_tracer(__name__)


def identity():
    return {
        "service": os.environ.get("SERVICE_NAME", "payment-producer"),
        "env": os.environ.get("ENV", "prod"),
        "version": os.environ.get("VERSION", "v2.0"),
        "change_id": os.environ.get("CHANGE_ID", "SD-M2-710"),
        "owner": os.environ.get("OWNER", "smartdine-ops"),
    }

def main():
    start_metrics(int(os.environ.get("METRICS_PORT", "9102")))
    tracer = setup_tracing()
    producer = make_producer(os.environ["KAFKA_BROKERS"])

    while True:
        with tracer.start_as_current_span("produce_payment_event"):
            order_id = f"ORD-{random.randint(10000,99999)}"
            payload = {
                "payment_id": f"PAY-{random.randint(10000,99999)}",
                "order_id": order_id,
                "status": random.choices(["started", "authorized", "failed"], weights=[3, 7, 2])[0],
                "latency_ms": random.random() * 800
            }
            evt = envelope("payment.event", identity(), payload)
            producer.send(TOPIC, value=evt)
            events_produced_total.labels(domain="payments").inc()
        safe_sleep(0.18)

if __name__ == "__main__":
    main()
