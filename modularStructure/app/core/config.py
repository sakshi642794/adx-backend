from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    APP_NAME: str = Field(default="Advenxure")
    ENVIRONMENT: str = Field(default="development")
    HOST: str = Field(default="0.0.0.0")
    PORT: int = Field(default=8000)
    DATABASE_URL: str = Field(
        default="postgresql+asyncpg://user:password@localhost:5432/advenxure"
    )

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
