from datetime import datetime, timezone
from typing import Dict, Any

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def envelope(event_type: str, identity: Dict[str, str], payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "event_type": event_type,
        "timestamp": now_iso(),
        "service": identity.get("service", ""),
        "env": identity.get("env", ""),
        "version": identity.get("version", ""),
        "change_id": identity.get("change_id", ""),
        "owner": identity.get("owner", ""),
        "payload": payload
    }
