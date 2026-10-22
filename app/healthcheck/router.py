import socket
from urllib.parse import urlparse

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from app.settings import settings
from app.storage import storage

router = APIRouter(prefix="/health", tags=["healthcheck"])


def check_tcp_endpoint(endpoint: str, timeout: float = 2.0) -> dict:
    target = endpoint.split(",", 1)[0].strip()
    parsed = urlparse(target if "://" in target else f"tcp://{target}")
    host = parsed.hostname
    port = parsed.port
    if not host or not port:
        return {"status": "failed", "error": f"Invalid endpoint: {endpoint}"}
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return {"status": "ok", "endpoint": f"{host}:{port}"}
    except OSError as exc:
        return {"status": "failed", "endpoint": f"{host}:{port}", "error": str(exc)}


@router.get("/")
@router.get("")
def health() -> dict:
    return {"status": "ok", "service": settings.service_name}


@router.get("/live/")
@router.get("/live")
def live() -> dict:
    return {"status": "ok", "service": settings.service_name, "checks": {"app": {"status": "ok"}}}


@router.get("/ready/")
@router.get("/ready")
def ready() -> JSONResponse:
    database = storage.check()
    checks = {
        "kafka": check_tcp_endpoint(settings.kafka_bootstrap_servers),
        "database": database,
    }
    ready_status = "ok" if all(check["status"] == "ok" for check in checks.values()) else "failed"
    return JSONResponse(
        status_code=status.HTTP_200_OK if ready_status == "ok" else status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"status": ready_status, "service": settings.service_name, "checks": checks},
    )
