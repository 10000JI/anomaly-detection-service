"""
Prometheus 서버 연결 설정
"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class PrometheusConfig:
    """Prometheus 연결 설정"""
    host: str
    port: int
    
    @classmethod
    def from_env(cls) -> 'PrometheusConfig':
        """환경 변수에서 설정 로드"""
        return cls(
            host=os.getenv('PROMETHEUS_HOST', ''),
            port=int(os.getenv('PROMETHEUS_PORT', '19090'))
        )
    
    def validate(self) -> None:
        """설정 검증 (필수값 체크)"""
        if not self.host:
            raise ValueError(
                "PROMETHEUS_HOST is not set. "
                "Please check .env.template file and set this value in .env file."
            )
    
    def get_prometheus_url(self) -> str:
        """Prometheus URL 반환"""
        return f"http://{self.host}:{self.port}"


# 싱글톤 패턴
_prometheus_config: Optional[PrometheusConfig] = None


def get_prometheus_config() -> PrometheusConfig:
    """싱글톤 패턴으로 Prometheus 설정 반환"""
    global _prometheus_config
    if _prometheus_config is None:
        _prometheus_config = PrometheusConfig.from_env()
        _prometheus_config.validate()
    return _prometheus_config

