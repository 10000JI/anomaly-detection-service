"""
MySQL 데이터베이스 연결 설정
"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class MySQLConfig:
    """MySQL 연결 설정 (Java의 @ConfigurationProperties와 유사)"""
    host: str
    port: int
    user: str
    password: str
    database: str
    pool_size: int
    pool_name: str
    
    @classmethod
    def from_env(cls) -> 'MySQLConfig':
        """환경 변수에서 설정 로드"""
        return cls(
            host=os.getenv('MYSQL_HOST', ''),
            port=int(os.getenv('MYSQL_PORT', '3306')),
            user=os.getenv('MYSQL_USER', ''),
            password=os.getenv('MYSQL_PASSWORD', ''),
            database=os.getenv('MYSQL_DATABASE', ''),
            pool_size=int(os.getenv('MYSQL_POOL_SIZE', '5')),
            pool_name=os.getenv('MYSQL_POOL_NAME', 'anomaly_pool')
        )
    
    def validate(self) -> None:
        """설정 검증 (필수값 체크)"""
        missing_fields = []
        
        if not self.host:
            missing_fields.append('MYSQL_HOST')
        if not self.user:
            missing_fields.append('MYSQL_USER')
        if not self.password:
            missing_fields.append('MYSQL_PASSWORD')
        if not self.database:
            missing_fields.append('MYSQL_DATABASE')
        
        if missing_fields:
            raise ValueError(
                f"Missing required MySQL configuration: {', '.join(missing_fields)}\n"
                "Please check .env.template file and set these values in .env file."
            )
    
    def get_connection_string(self) -> str:
        """MySQL 연결 문자열 반환"""
        return f"mysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


# 싱글톤 패턴
_mysql_config: Optional[MySQLConfig] = None


def get_mysql_config() -> MySQLConfig:
    """싱글톤 패턴으로 MySQL 설정 반환"""
    global _mysql_config
    if _mysql_config is None:
        _mysql_config = MySQLConfig.from_env()
        _mysql_config.validate()
    return _mysql_config

