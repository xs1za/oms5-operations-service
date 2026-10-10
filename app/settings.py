from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    service_name: str = "OMS5"
    root_path: str = ""
    kafka_bootstrap_servers: str = "kafka.oms.svc.cluster.local:9092"
    oms1_auth_base_url: str = "http://oms1.oms.svc.cluster.local"
    service_client_id: str = "OMS5"
    service_client_secret: str = ""


settings = Settings()
