from prometheus_client import Counter, start_http_server

events_produced_total = Counter("events_produced_total", "Events produced", ["domain"])
dq_failed_total = Counter("dq_failed_total", "DQ failures total")
dq_passed_total = Counter("dq_passed_total", "DQ passes total")
incidents_emitted_total = Counter("incidents_emitted_total", "Incident candidates emitted")

def start_metrics(port: int):
    start_http_server(port)
