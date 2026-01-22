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

TOPIC = "smartdine.orders"

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


def base_identity():
    return {
        "service": os.environ.get("SERVICE_NAME", "order-producer"),
        "env": os.environ.get("ENV", "prod"),
        "version": os.environ.get("VERSION", "v2.0"),
        "change_id": os.environ.get("CHANGE_ID", "SD-M2-710"),
        "owner": os.environ.get("OWNER", "smartdine-ops"),
    }

def maybe_break_identity(idt: dict) -> dict:
    strict = os.environ.get("STRICT_CONTRACT", "true").lower() == "true"
    if strict:
        return idt
    # realistic failure: missing identity fields in some events
    out = dict(idt)
    if random.random() < 0.20:
        out["owner"] = ""
    if random.random() < 0.10:
        out["change_id"] = ""
    return out

def main():
    start_metrics(int(os.environ.get("METRICS_PORT", "9101")))
    tracer = setup_tracing()
    producer = make_producer(os.environ["KAFKA_BROKERS"])

    branch_ids = ["BLR-01", "HYD-02", "DEL-03", "MUM-04"]

    while True:
        with tracer.start_as_current_span("produce_order_event"):
            order_id = f"ORD-{random.randint(10000,99999)}"
            payload = {
                "order_id": order_id,
                "branch_id": random.choice(branch_ids),
                "status": random.choice(["placed", "accepted", "prepping", "ready"]),
                "prep_seconds": random.random() * 60
            }
            evt = envelope("order.event", maybe_break_identity(base_identity()), payload)
            producer.send(TOPIC, value=evt)
            events_produced_total.labels(domain="orders").inc()
        safe_sleep(0.15)

if __name__ == "__main__":
    main()
