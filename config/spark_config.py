"""
PySpark 로컬 실행 설정
"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class SparkConfig:
    """PySpark 로컬 실행 설정"""
    app_name: str
    master: str
    executor_memory: str
    driver_memory: str
    log_level: str
    
    @classmethod
    def from_env(cls) -> 'SparkConfig':
        """환경 변수에서 설정 로드"""
        return cls(
            app_name=os.getenv('SPARK_APP_NAME', ''),
            master=os.getenv('SPARK_MASTER', 'local[*]'),
            executor_memory=os.getenv('SPARK_EXECUTOR_MEMORY', '2g'),
            driver_memory=os.getenv('SPARK_DRIVER_MEMORY', '1g'),
            log_level=os.getenv('SPARK_LOG_LEVEL', 'WARN')
        )
    
    def validate(self) -> None:
        """설정 검증 (필수값 체크)"""
        if not self.app_name:
            raise ValueError(
                "SPARK_APP_NAME is not set. "
                "Please check .env.template file and set this value in .env file."
            )
        if not self.master.startswith('local'):
            raise ValueError(
                "This system only supports local Spark execution. "
                "SPARK_MASTER must start with 'local' (e.g., local[*]). "
                "Please check .env.template file."
            )
    
    def get_spark_conf_dict(self) -> dict:
        """Spark 설정 딕셔너리 반환"""
        return {
            'spark.app.name': self.app_name,
            'spark.master': self.master,
            'spark.executor.memory': self.executor_memory,
            'spark.driver.memory': self.driver_memory,
        }


# 싱글톤 패턴
_spark_config: Optional[SparkConfig] = None


def get_spark_config() -> SparkConfig:
    """싱글톤 패턴으로 Spark 설정 반환"""
    global _spark_config
    if _spark_config is None:
        _spark_config = SparkConfig.from_env()
        _spark_config.validate()
    return _spark_config

