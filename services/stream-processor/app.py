import os
import time
import yaml
from collections import deque

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes


from common.kafka import make_consumer, make_producer, safe_sleep
from common.metrics import start_metrics, incidents_emitted_total

ENRICHED_TOPIC = "smartdine.enriched"
INC_TOPIC = "smartdine.incidents"

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


def load_catalog():
    with open("/configs/service_catalog.yml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    start_metrics(int(os.environ.get("METRICS_PORT", "9105")))
    tracer = setup_tracing()

    brokers = os.environ["KAFKA_BROKERS"]
    producer = make_producer(brokers)

    catalog = load_catalog()
    orders_topic = catalog["domains"]["orders"]["topic"]
    payments_topic = catalog["domains"]["payments"]["topic"]

    orders_consumer = make_consumer(brokers, orders_topic, group_id="proc-orders")
    payments_consumer = make_consumer(brokers, payments_topic, group_id="proc-payments")

    recent_orders = deque(maxlen=200)
    recent_payments = deque(maxlen=200)

    while True:
        any_msg = False
        with tracer.start_as_current_span("stream_processor_loop"):
            for msg in orders_consumer:
                any_msg = True
                evt = msg.value
                enriched = dict(evt)
                enriched["mesh_domain"] = "orders"
                enriched["mesh_owner"] = catalog["domains"]["orders"]["owner"]
                producer.send(ENRICHED_TOPIC, value=enriched)
                recent_orders.append(enriched)

            for msg in payments_consumer:
                any_msg = True
                evt = msg.value
                enriched = dict(evt)
                enriched["mesh_domain"] = "payments"
                enriched["mesh_owner"] = catalog["domains"]["payments"]["owner"]
                producer.send(ENRICHED_TOPIC, value=enriched)
                recent_payments.append(enriched)

            # Streaming correlation: if payment failures spike AND slow orders spike → incident candidate
            fails = [p for p in recent_payments if p.get("payload", {}).get("status") == "failed"]
            slow = [o for o in recent_orders if o.get("payload", {}).get("prep_seconds", 0) > 35]

            if len(fails) >= 10 and len(slow) >= 10:
                incident = {
                    "incident_type": "peak_hour_degradation",
                    "timestamp": time.time(),
                    "signals": {
                        "payment_failed_count": len(fails),
                        "slow_orders_count": len(slow)
                    },
                    "evidence": {
                        "sample_change_id": fails[-1].get("change_id", ""),
                        "sample_owner": fails[-1].get("owner", ""),
                        "note": "Streaming incident candidate (EDA + stream processing), not ML."
                    }
                }
                producer.send(INC_TOPIC, value=incident)
                incidents_emitted_total.inc()
                recent_orders.clear()
                recent_payments.clear()

        if not any_msg:
            safe_sleep(0.4)

if __name__ == "__main__":
    main()
