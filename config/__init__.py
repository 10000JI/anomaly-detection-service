"""
설정 패키지 초기화
.env 파일을 자동으로 로드합니다.
"""
from dotenv import load_dotenv
import os
from pathlib import Path

# 프로젝트 루트에서 .env 파일 로드
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# 설정 클래스 임포트
from .mysql_config import MySQLConfig, get_mysql_config
from .kafka_config import KafkaConfig, get_kafka_config
from .redis_config import RedisConfig, get_redis_config
from .spark_config import SparkConfig, get_spark_config
from .prometheus_config import PrometheusConfig, get_prometheus_config

__all__ = [
    'MySQLConfig', 'get_mysql_config',
    'KafkaConfig', 'get_kafka_config',
    'RedisConfig', 'get_redis_config',
    'SparkConfig', 'get_spark_config',
    'PrometheusConfig', 'get_prometheus_config'
]



