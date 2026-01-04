"""
Kafka Consumer 단위 테스트

- KafkaEventConsumer 클래스 테스트
- RedisClient 클래스 테스트
- Mock을 사용한 외부 서비스 테스트
- 에러 핸들링 시나리오 테스트
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock, patch, call

import pytest


# 프로젝트 루트를 sys.path에 추가
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


# 샘플 이벤트 데이터
SAMPLE_EVENT = {
    "event_id": "evt-test-001",
    "timestamp": "2025-01-03T10:30:45.123Z",
    "user_id": "user-001",
    "session_id": "sess-abc-001",
    "event_type": "watch",
    "content_id": "movie-001",
    "genre": "action",
    "duration_minutes": 120,
    "watched_minutes": 45,
    "metadata": {
        "user_segment": "VIP",
        "ab_test_group": "A",
        "content_type": "movie",
        "device": "mobile",
    },
}


class TestKafkaEventConsumer:
    """Kafka Event Consumer 단위 테스트"""
    
    @patch("src.kafka.consumer.MySQLClient")
    @patch("src.kafka.consumer.get_kafka_config")
    def test_initialization(self, mock_kafka_config, mock_mysql):
        """Consumer 초기화 테스트"""
        from src.kafka.consumer import KafkaEventConsumer
        
        # Mock 설정
        mock_config = MagicMock()
        mock_config.topic = "test-topic"
        mock_kafka_config.return_value = mock_config
        
        # 테스트
        consumer = KafkaEventConsumer(batch_size=50, batch_timeout=10)
        
        # 검증
        assert consumer.batch_size == 50
        assert consumer.batch_timeout == 10
        assert consumer.total_consumed == 0
        assert len(consumer.batch_buffer) == 0
    
    @patch("src.kafka.consumer.MySQLClient")
    @patch("src.kafka.consumer.get_kafka_config")
    def test_parse_event_valid(self, mock_kafka_config, mock_mysql):
        """유효한 이벤트 파싱 테스트"""
        from src.kafka.consumer import KafkaEventConsumer
        
        # Mock 설정
        mock_config = MagicMock()
        mock_kafka_config.return_value = mock_config
        
        consumer = KafkaEventConsumer()
        
        # Mock 메시지
        mock_message = MagicMock()
        mock_message.value = json.dumps(SAMPLE_EVENT)
        
        # 테스트
        result = consumer.parse_event(mock_message)
        
        # 검증
        assert result is not None
        assert result["event_id"] == "evt-test-001"
        assert result["user_id"] == "user-001"
    
    @patch("src.kafka.consumer.MySQLClient")
    @patch("src.kafka.consumer.get_kafka_config")
    def test_parse_event_invalid_json(self, mock_kafka_config, mock_mysql):
        """잘못된 JSON 파싱 테스트"""
        from src.kafka.consumer import KafkaEventConsumer
        
        # Mock 설정
        mock_config = MagicMock()
        mock_kafka_config.return_value = mock_config
        
        consumer = KafkaEventConsumer()
        
        # Mock 메시지 (잘못된 JSON)
        mock_message = MagicMock()
        mock_message.value = "invalid json{}"
        
        # 테스트
        result = consumer.parse_event(mock_message)
        
        # 검증
        assert result is None
    
    @patch("src.kafka.consumer.MySQLClient")
    @patch("src.kafka.consumer.get_kafka_config")
    def test_validate_event_success(self, mock_kafka_config, mock_mysql):
        """이벤트 유효성 검증 성공 테스트"""
        from src.kafka.consumer import KafkaEventConsumer
        
        # Mock 설정
        mock_config = MagicMock()
        mock_kafka_config.return_value = mock_config
        
        consumer = KafkaEventConsumer()
        
        # 테스트
        result = consumer.validate_event(SAMPLE_EVENT)
        
        # 검증
        assert result is True
    
    @patch("src.kafka.consumer.MySQLClient")
    @patch("src.kafka.consumer.get_kafka_config")
    def test_validate_event_missing_field(self, mock_kafka_config, mock_mysql):
        """필수 필드 누락 이벤트 검증 테스트"""
        from src.kafka.consumer import KafkaEventConsumer
        
        # Mock 설정
        mock_config = MagicMock()
        mock_kafka_config.return_value = mock_config
        
        consumer = KafkaEventConsumer()
        
        # 필수 필드 제거
        invalid_event = SAMPLE_EVENT.copy()
        del invalid_event["user_id"]
        
        # 테스트
        result = consumer.validate_event(invalid_event)
        
        # 검증
        assert result is False
    
    @patch("src.kafka.consumer.MySQLClient")
    @patch("src.kafka.consumer.get_kafka_config")
    def test_validate_event_invalid_type(self, mock_kafka_config, mock_mysql):
        """잘못된 event_type 검증 테스트"""
        from src.kafka.consumer import KafkaEventConsumer
        
        # Mock 설정
        mock_config = MagicMock()
        mock_kafka_config.return_value = mock_config
        
        consumer = KafkaEventConsumer()
        
        # event_type을 잘못된 값으로 변경
        invalid_event = SAMPLE_EVENT.copy()
        invalid_event["event_type"] = "invalid_type"
        
        # 테스트
        result = consumer.validate_event(invalid_event)
        
        # 검증
        assert result is False
    
    @patch("src.kafka.consumer.MySQLClient")
    @patch("src.kafka.consumer.get_kafka_config")
    def test_add_to_batch(self, mock_kafka_config, mock_mysql):
        """배치 버퍼에 이벤트 추가 테스트"""
        from src.kafka.consumer import KafkaEventConsumer
        
        # Mock 설정
        mock_config = MagicMock()
        mock_kafka_config.return_value = mock_config
        
        consumer = KafkaEventConsumer()
        
        # 테스트
        consumer.add_to_batch(SAMPLE_EVENT)
        
        # 검증
        assert len(consumer.batch_buffer) == 1
        assert consumer.batch_buffer[0] == SAMPLE_EVENT
    
    @patch("src.kafka.consumer.MySQLClient")
    @patch("src.kafka.consumer.get_kafka_config")
    def test_should_flush_by_size(self, mock_kafka_config, mock_mysql):
        """배치 크기 도달 시 플러시 판단 테스트"""
        from src.kafka.consumer import KafkaEventConsumer
        
        # Mock 설정
        mock_config = MagicMock()
        mock_kafka_config.return_value = mock_config
        
        consumer = KafkaEventConsumer(batch_size=2)
        
        # 배치 크기만큼 추가
        consumer.add_to_batch(SAMPLE_EVENT)
        consumer.add_to_batch(SAMPLE_EVENT)
        
        # 테스트
        result = consumer.should_flush()
        
        # 검증
        assert result is True
    
    @patch("src.kafka.consumer.MySQLClient")
    @patch("src.kafka.consumer.get_kafka_config")
    def test_should_flush_by_timeout(self, mock_kafka_config, mock_mysql):
        """배치 타임아웃 시 플러시 판단 테스트"""
        from src.kafka.consumer import KafkaEventConsumer
        import time
        
        # Mock 설정
        mock_config = MagicMock()
        mock_kafka_config.return_value = mock_config
        
        consumer = KafkaEventConsumer(batch_size=100, batch_timeout=1)
        
        # 1개만 추가 (크기 미달)
        consumer.add_to_batch(SAMPLE_EVENT)
        
        # 타임아웃 대기
        time.sleep(1.1)
        
        # 테스트
        result = consumer.should_flush()
        
        # 검증
        assert result is True
    
    @patch("src.kafka.consumer.MySQLClient")
    @patch("src.kafka.consumer.get_kafka_config")
    def test_process_event_with_redis(self, mock_kafka_config, mock_mysql):
        """이벤트 처리 테스트"""
        from src.kafka.consumer import KafkaEventConsumer
        
        # Mock 설정
        mock_config = MagicMock()
        mock_kafka_config.return_value = mock_config
        
        consumer = KafkaEventConsumer()
        
        # 테스트
        consumer.process_event(SAMPLE_EVENT)
        
        # 검증
        assert len(consumer.batch_buffer) == 1
    
    @patch("src.kafka.consumer.MySQLClient")
    @patch("src.kafka.consumer.get_kafka_config")
    def test_flush_batch_success(self, mock_kafka_config, mock_mysql):
        """배치 플러시 성공 테스트"""
        from src.kafka.consumer import KafkaEventConsumer
        
        # Mock 설정
        mock_config = MagicMock()
        mock_kafka_config.return_value = mock_config
        
        mock_mysql_instance = MagicMock()
        mock_mysql.return_value = mock_mysql_instance
        mock_mysql_instance.batch_insert_user_events.return_value = 2
        
        consumer = KafkaEventConsumer()
        consumer.mysql_client = mock_mysql_instance
        
        # 배치에 이벤트 추가
        consumer.add_to_batch(SAMPLE_EVENT)
        consumer.add_to_batch(SAMPLE_EVENT)
        
        # 테스트
        consumer.flush_batch()
        
        # 검증
        assert len(consumer.batch_buffer) == 0
        assert consumer.total_saved == 2
        mock_mysql_instance.batch_insert_user_events.assert_called_once()
    
    @patch("src.kafka.consumer.MySQLClient")
    @patch("src.kafka.consumer.get_kafka_config")
    def test_commit_offset_success(self, mock_kafka_config, mock_mysql):
        """오프셋 커밋 성공 테스트"""
        from src.kafka.consumer import KafkaEventConsumer
        
        # Mock 설정
        mock_config = MagicMock()
        mock_kafka_config.return_value = mock_config
        
        consumer = KafkaEventConsumer()
        consumer.consumer = MagicMock()
        
        # 테스트
        consumer.commit_offset()
        
        # 검증
        consumer.consumer.commit.assert_called_once()


class TestIntegration:
    """통합 테스트 시나리오"""
    
    @patch("src.kafka.consumer.KafkaConsumer")
    @patch("src.kafka.consumer.MySQLClient")
    @patch("src.kafka.consumer.get_kafka_config")
    def test_full_event_flow(
        self,
        mock_kafka_config,
        mock_mysql,
        mock_kafka_consumer,
    ):
        """전체 이벤트 처리 플로우 테스트"""
        from src.kafka.consumer import KafkaEventConsumer
        
        # Mock 설정
        mock_config = MagicMock()
        mock_config.topic = "test-topic"
        mock_config.consumer_group = "test-group"
        mock_config.get_bootstrap_servers_list.return_value = ["localhost:9092"]
        mock_kafka_config.return_value = mock_config
        
        # MySQL Mock
        mock_mysql_instance = MagicMock()
        mock_mysql.return_value = mock_mysql_instance
        mock_mysql_instance.batch_insert_user_events.return_value = 2
        
        # Kafka Consumer Mock
        mock_consumer_instance = MagicMock()
        mock_kafka_consumer.return_value = mock_consumer_instance
        
        # Mock 메시지 생성
        mock_message1 = MagicMock()
        mock_message1.value = json.dumps(SAMPLE_EVENT)
        
        mock_message2 = MagicMock()
        event2 = SAMPLE_EVENT.copy()
        event2["event_id"] = "evt-test-002"
        mock_message2.value = json.dumps(event2)
        
        # Consumer가 2개의 메시지를 반환한 후 종료
        mock_consumer_instance.__iter__.return_value = iter([mock_message1, mock_message2])
        
        # 테스트
        consumer = KafkaEventConsumer(batch_size=2)
        consumer.consumer = mock_consumer_instance
        consumer.mysql_client = mock_mysql_instance
        
        # 이벤트 처리
        for message in [mock_message1, mock_message2]:
            event = consumer.parse_event(message)
            if event and consumer.validate_event(event):
                consumer.process_event(event)
        
        # 배치 플러시
        if consumer.should_flush():
            consumer.flush_batch()
            consumer.commit_offset()
        
        # 검증
        assert consumer.total_consumed == 0  # consume_events를 직접 호출하지 않았으므로
        assert len(consumer.batch_buffer) == 0  # 플러시됨
        mock_mysql_instance.batch_insert_user_events.assert_called_once()


def test_sample_event_structure():
    """샘플 이벤트 구조 검증 테스트"""
    required_fields = [
        "event_id",
        "timestamp",
        "user_id",
        "session_id",
        "event_type",
        "content_id",
    ]
    
    for field in required_fields:
        assert field in SAMPLE_EVENT, f"Missing required field: {field}"
    
    assert SAMPLE_EVENT["event_type"] in [
        "click",
        "watch",
        "watchlist",
        "watch_complete",
        "rating",
    ]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])



