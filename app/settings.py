from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    service_name: str = "OMS5"
    root_path: str = ""
    kafka_bootstrap_servers: str = "kafka.oms.svc.cluster.local:9092"
    oms1_auth_base_url: str = "http://oms1.oms.svc.cluster.local"
    service_client_id: str = "OMS5"
    service_client_secret: str = ""
    oms2_service_secret: str = ""
    database_url: str = "postgresql://oms5:oms5@oms5-postgres:5432/oms5"
    enable_funnel_outbox: str = "true"
    funnel_outbox_batch_size: int = 100
    funnel_outbox_poll_interval: float = 1.0


settings = Settings()
