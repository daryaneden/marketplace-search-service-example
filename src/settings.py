from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    database_url: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5435/search_db",
        validation_alias=AliasChoices(
            "POSTGRES_CONNECTION_STRING",
            "DATABASE_URL",
            "database_url",
        ),
    )
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_ads: str = "ads"
    kafka_consumer_group: str = "search-service"
    ad_service_url: str = "http://localhost:8002"

    @field_validator("database_url", mode="before")
    @classmethod
    def normalize_database_url(cls, value: str) -> str:
        if isinstance(value, str):
            if value.startswith("postgres://"):
                return value.replace(
                    "postgres://",
                    "postgresql+asyncpg://",
                    1,
                )

            if value.startswith("postgresql://"):
                return value.replace(
                    "postgresql://",
                    "postgresql+asyncpg://",
                    1,
                )

        return value
