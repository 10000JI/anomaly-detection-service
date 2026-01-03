"""
Redis 캐시 서버 연결 설정
"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class RedisConfig:
    """Redis 캐시 서버 연결 설정"""
    host: str
    port: int
    db: int
    password: str
    ttl: int
    
    @classmethod
    def from_env(cls) -> 'RedisConfig':
        """환경 변수에서 설정 로드"""
        return cls(
            host=os.getenv('REDIS_HOST', ''),
            port=int(os.getenv('REDIS_PORT', '6379')),
            db=int(os.getenv('REDIS_DB', '0')),
            password=os.getenv('REDIS_PASSWORD', ''),
            ttl=int(os.getenv('REDIS_TTL', '600'))
        )
    
    def validate(self) -> None:
        """설정 검증 (필수값 체크)"""
        if not self.host:
            raise ValueError(
                "REDIS_HOST is not set. "
                "Please check .env.template file and set this value in .env file."
            )
    
    def get_redis_url(self) -> str:
        """Redis 연결 URL 반환"""
        if self.password:
            return f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"


# 싱글톤 패턴
_redis_config: Optional[RedisConfig] = None


def get_redis_config() -> RedisConfig:
    """싱글톤 패턴으로 Redis 설정 반환"""
    global _redis_config
    if _redis_config is None:
        _redis_config = RedisConfig.from_env()
        _redis_config.validate()
    return _redis_config

