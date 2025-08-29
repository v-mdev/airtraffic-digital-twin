import os
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

DOTENV = os.path.join(os.path.dirname(__file__), ".env")

class Settings(BaseSettings):
    username: str = Field(default="", alias="OPENSKY_USERNAME")
    password: str = Field(default="", alias="OPENSKY_PASSWORD")
    redis_username: str = Field(default="", alias="REDIS_USERNAME")
    redis_password: str = Field(default="", alias="REDIS_PASSWORD")
    redis_host: str = Field(default="", alias="REDIS_HOST")
    redis_port: int = Field(default=14436, alias="REDIS_PORT")

    model_config = SettingsConfigDict(env_file=DOTENV, env_file_encoding='utf-8')

settings = Settings()
