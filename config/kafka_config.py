"""
Kafka 클러스터 연결 설정
"""
import os
from dataclasses import dataclass
from typing import Optional, List


@dataclass
class KafkaConfig:
    """Kafka 연결 설정"""
    bootstrap_servers: str
    topic: str
    consumer_group: str
    auto_offset_reset: str
    enable_auto_commit: bool
    
    @classmethod
    def from_env(cls) -> 'KafkaConfig':
        """환경 변수에서 설정 로드"""
        return cls(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', ''),
            topic=os.getenv('KAFKA_TOPIC', ''),
            consumer_group=os.getenv('KAFKA_CONSUMER_GROUP', ''),
            auto_offset_reset=os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest'),
            enable_auto_commit=os.getenv('KAFKA_ENABLE_AUTO_COMMIT', 'false').lower() == 'true'
        )
    
    def validate(self) -> None:
        """설정 검증 (필수값 체크)"""
        missing_fields = []
        
        if not self.bootstrap_servers:
            missing_fields.append('KAFKA_BOOTSTRAP_SERVERS')
        if not self.topic:
            missing_fields.append('KAFKA_TOPIC')
        if not self.consumer_group:
            missing_fields.append('KAFKA_CONSUMER_GROUP')
        
        if missing_fields:
            raise ValueError(
                f"Missing required Kafka configuration: {', '.join(missing_fields)}\n"
                "Please check .env.template file and set these values in .env file."
            )
    
    def get_bootstrap_servers_list(self) -> List[str]:
        """부트스트랩 서버 리스트 반환"""
        return [server.strip() for server in self.bootstrap_servers.split(',')]


# 싱글톤 패턴
_kafka_config: Optional[KafkaConfig] = None


def get_kafka_config() -> KafkaConfig:
    """싱글톤 패턴으로 Kafka 설정 반환"""
    global _kafka_config
    if _kafka_config is None:
        _kafka_config = KafkaConfig.from_env()
        _kafka_config.validate()
    return _kafka_config

