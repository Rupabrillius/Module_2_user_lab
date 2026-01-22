import os
import yaml

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes


from common.kafka import make_consumer, make_producer, safe_sleep
from common.contracts import load_schema, validate
from common.metrics import start_metrics, dq_failed_total, dq_passed_total

DQ_TOPIC = "smartdine.dq"

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
    start_metrics(int(os.environ.get("METRICS_PORT", "9104")))
    tracer = setup_tracing()

    brokers = os.environ["KAFKA_BROKERS"]
    producer = make_producer(brokers)

    catalog = load_catalog()
    topics = [catalog["domains"]["orders"]["topic"], catalog["domains"]["payments"]["topic"]]
    consumers = [make_consumer(brokers, t, group_id=f"dq-{t}") for t in topics]

    envelope_schema = load_schema("/configs/contracts/event_envelope.schema.json")
    orders_schema = load_schema("/configs/contracts/orders.schema.json")
    payments_schema = load_schema("/configs/contracts/payments.schema.json")

    while True:
        any_msg = False
        with tracer.start_as_current_span("dq_loop"):
            for c in consumers:
                for msg in c:
                    any_msg = True
                    evt = msg.value
                    errors = validate(envelope_schema, evt)

                    payload = evt.get("payload", {})
                    if msg.topic.endswith("orders"):
                        errors += validate(orders_schema, payload)
                    if msg.topic.endswith("payments"):
                        errors += validate(payments_schema, payload)

                    dq_event = {
                        "topic": msg.topic,
                        "status": "passed" if not errors else "failed",
                        "errors": errors[:10],
                        "sample_identity": {
                            "service": evt.get("service"),
                            "env": evt.get("env"),
                            "version": evt.get("version"),
                            "change_id": evt.get("change_id"),
                            "owner": evt.get("owner")
                        }
                    }

                    if errors:
                        dq_failed_total.inc()
                    else:
                        dq_passed_total.inc()

                    producer.send(DQ_TOPIC, value=dq_event)

        if not any_msg:
            safe_sleep(0.4)

if __name__ == "__main__":
    main()
