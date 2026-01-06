"""
Kafka Consumer 통합 테스트

- Producer로 이벤트 생성 및 전송
- Consumer 실행 및 MySQL 저장 확인
- 시간 간격을 두고 추가 이벤트 전송 및 검증

실행 방법:
    pytest tests/test_kafka_consumer_integration.py -v
    pytest tests/test_kafka_consumer_integration.py::test_consumer_batch_save -v
"""

from __future__ import annotations

import json
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from uuid import uuid4

import pytest

# 프로젝트 루트를 sys.path에 추가
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from kafka import KafkaProducer


def create_test_event(
    user_id: str = "test-user-001",
    session_id: str = "test-session-001",
    event_type: str = "click",
    content_id: str = "test-content-001",
    watched_minutes: int = 0,
) -> Dict[str, Any]:
    """
    테스트용 이벤트 생성
    
    Args:
        user_id: 사용자 ID
        session_id: 세션 ID
        event_type: 이벤트 타입
        content_id: 콘텐츠 ID
        watched_minutes: 시청 시간
        
    Returns:
        Dict: 이벤트 딕셔너리
    """
    return {
        "event_id": str(uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "user_id": user_id,
        "session_id": session_id,
        "event_type": event_type,
        "content_id": content_id,
        "genre": "action",
        "duration_minutes": 120,
        "watched_minutes": watched_minutes,
        "metadata": {
            "user_segment": "VIP",
            "ab_test_group": "A",
            "content_type": "movie",
            "device": "mobile",
        },
    }


def send_events_to_kafka(events: List[Dict[str, Any]], topic: str) -> None:
    """
    Kafka에 이벤트 전송
    
    Args:
        events: 전송할 이벤트 리스트
        topic: Kafka 토픽 이름
    """
    from config import get_kafka_config
    
    kafka_config = get_kafka_config()
    bootstrap_servers = kafka_config.get_bootstrap_servers_list()
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else None,
    )
    
    try:
        for event in events:
            producer.send(topic, key=event["user_id"], value=event)
        
        producer.flush(timeout=10)
        print(f"✓ {len(events)}개 이벤트 전송 완료")
    finally:
        producer.close()


def get_user_events_count(mysql_client, user_id: str = None) -> int:
    """
    MySQL user_events 테이블에서 이벤트 수 조회
    
    Args:
        mysql_client: MySQL 클라이언트
        user_id: 사용자 ID (None이면 전체)
        
    Returns:
        int: 이벤트 수
    """
    if user_id:
        query = "SELECT COUNT(*) as cnt FROM user_events WHERE user_id = %s"
        result = mysql_client.fetch_one(query, (user_id,))
    else:
        query = "SELECT COUNT(*) as cnt FROM user_events"
        result = mysql_client.fetch_one(query)
    
    if result and 'cnt' in result:
        return result['cnt']
    return 0


def run_consumer_in_background(consumer, duration: int = 10) -> threading.Thread:
    """
    Consumer를 백그라운드에서 실행
    
    Args:
        consumer: KafkaEventConsumer 인스턴스
        duration: 실행 시간 (초)
        
    Returns:
        threading.Thread: 실행 중인 스레드
    """
    def consumer_worker():
        try:
            consumer.connect()
            # 짧은 시간 동안만 실행
            start_time = time.time()
            for message in consumer.consumer:
                if time.time() - start_time > duration:
                    break
                try:
                    event = consumer.parse_event(message)
                    if event and consumer.validate_event(event):
                        consumer.process_event(event)
                        consumer.total_consumed += 1
                        if consumer.should_flush():
                            consumer.flush_batch()
                            consumer.commit_offset()
                except Exception as e:
                    consumer.total_errors += 1
                    print(f"⚠ Consumer 오류: {e}")
        except Exception as e:
            print(f"⚠ Consumer 실행 오류: {e}")
        finally:
            if consumer.batch_buffer:
                consumer.flush_batch()
                consumer.commit_offset()
            consumer.close()
    
    thread = threading.Thread(target=consumer_worker, daemon=True)
    thread.start()
    return thread


@pytest.mark.integration
class TestKafkaConsumerIntegration:
    """Kafka Consumer 통합 테스트"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """테스트 전 설정"""
        from config import get_kafka_config
        from src.storage.mysql_client import MySQLClient
        
        self.kafka_config = get_kafka_config()
        self.mysql_client = MySQLClient()
        self.mysql_client.connect()
        
        # 테스트 전 초기 카운트 저장
        self.initial_count = get_user_events_count(self.mysql_client)
        
        yield
        
        # 테스트 후 정리: 테스트 데이터 삭제
        try:
            # 테스트용 user_id로 저장된 이벤트 삭제
            test_user_ids = [
                "test-user-001", "test-user-002", "test-user-003", "test-user-004"
            ]
            for user_id in test_user_ids:
                delete_query = "DELETE FROM user_events WHERE user_id = %s"
                try:
                    self.mysql_client.execute_query(delete_query, (user_id,))
                except:
                    pass
            self.mysql_client.close()
        except:
            pass
    
    def test_consumer_initial_batch(self):
        """초기 5건 이벤트 전송 및 Consumer 저장 테스트"""
        from src.kafka.consumer import KafkaEventConsumer
        
        # 1. 초기 5건 이벤트 생성 및 전송
        events = [
            create_test_event(
                user_id="test-user-001",
                session_id="test-session-001",
                event_type="click",
                content_id=f"content-{i:03d}",
            )
            for i in range(5)
        ]
        
        print(f"\n[1단계] 초기 5건 이벤트 전송...")
        send_events_to_kafka(events, self.kafka_config.topic)
        time.sleep(1)  # 전송 완료 대기
        
        # 2. Consumer 실행
        consumer = KafkaEventConsumer(batch_size=5, batch_timeout=3)
        print(f"[2단계] Consumer 실행 (배치 크기: 5, 타임아웃: 3초)...")
        
        consumer_thread = run_consumer_in_background(consumer, duration=5)
        time.sleep(6)  # Consumer 처리 대기
        
        # 3. MySQL 저장 확인
        final_count = get_user_events_count(self.mysql_client)
        new_events = final_count - self.initial_count
        
        print(f"[3단계] MySQL 저장 확인:")
        print(f"  - 초기 이벤트 수: {self.initial_count}")
        print(f"  - 최종 이벤트 수: {final_count}")
        print(f"  - 새로 저장된 이벤트: {new_events}")
        
        # 검증: 최소 5건 이상 저장되어야 함
        assert new_events >= 5, f"예상: 5건 이상, 실제: {new_events}건"
        
        # 특정 사용자 이벤트 확인
        user_count = get_user_events_count(self.mysql_client, "test-user-001")
        assert user_count >= 5, f"test-user-001의 이벤트가 5건 이상 저장되어야 함 (실제: {user_count}건)"
        
        print(f"✓ 초기 배치 테스트 통과: {new_events}건 저장됨")
    
    def test_consumer_delayed_batch(self):
        """2초 간격으로 추가 이벤트 전송 및 저장 테스트"""
        from src.kafka.consumer import KafkaEventConsumer
        
        # 1. 초기 5건 이벤트 전송
        initial_events = [
            create_test_event(
                user_id="test-user-002",
                session_id="test-session-002",
                event_type="click",
                content_id=f"content-{i:03d}",
            )
            for i in range(5)
        ]
        
        print(f"\n[1단계] 초기 5건 이벤트 전송...")
        send_events_to_kafka(initial_events, self.kafka_config.topic)
        time.sleep(1)
        
        # 2. Consumer 실행
        consumer = KafkaEventConsumer(batch_size=5, batch_timeout=3)
        print(f"[2단계] Consumer 실행...")
        
        consumer_thread = run_consumer_in_background(consumer, duration=10)
        time.sleep(2)  # 초기 배치 처리 대기
        
        # 3. 초기 배치 저장 확인
        count_after_initial = get_user_events_count(self.mysql_client)
        new_after_initial = count_after_initial - self.initial_count
        print(f"[3단계] 초기 배치 저장 확인: {new_after_initial}건")
        
        # 4. 2초 후 추가 5건 이벤트 전송
        print(f"[4단계] 2초 대기 후 추가 5건 이벤트 전송...")
        time.sleep(2)
        
        additional_events = [
            create_test_event(
                user_id="test-user-002",
                session_id="test-session-002",
                event_type="watch",
                content_id=f"content-{i+5:03d}",
                watched_minutes=10,
            )
            for i in range(5)
        ]
        
        send_events_to_kafka(additional_events, self.kafka_config.topic)
        time.sleep(5)  # 추가 배치 처리 대기
        
        # 5. 최종 저장 확인
        final_count = get_user_events_count(self.mysql_client)
        new_events_total = final_count - self.initial_count
        
        print(f"[5단계] 최종 저장 확인:")
        print(f"  - 초기 이벤트 수: {self.initial_count}")
        print(f"  - 최종 이벤트 수: {final_count}")
        print(f"  - 총 새로 저장된 이벤트: {new_events_total}")
        
        # 검증: 최소 10건 이상 저장되어야 함
        assert new_events_total >= 10, f"예상: 10건 이상, 실제: {new_events_total}건"
        
        # 특정 사용자 이벤트 확인
        user_count = get_user_events_count(self.mysql_client, "test-user-002")
        assert user_count >= 10, f"test-user-002의 이벤트가 10건 이상 저장되어야 함 (실제: {user_count}건)"
        
        print(f"✓ 지연 배치 테스트 통과: 총 {new_events_total}건 저장됨")
    
    def test_consumer_batch_timeout(self):
        """배치 타임아웃 테스트 (작은 배치 크기, 타임아웃으로 플러시)"""
        from src.kafka.consumer import KafkaEventConsumer
        
        # 1. 3건 이벤트 전송 (배치 크기보다 작음)
        events = [
            create_test_event(
                user_id="test-user-003",
                session_id="test-session-003",
                event_type="click",
                content_id=f"content-{i:03d}",
            )
            for i in range(3)
        ]
        
        print(f"\n[1단계] 3건 이벤트 전송 (배치 크기 5보다 작음)...")
        send_events_to_kafka(events, self.kafka_config.topic)
        time.sleep(1)
        
        # 2. Consumer 실행 (배치 크기 5, 타임아웃 3초)
        consumer = KafkaEventConsumer(batch_size=5, batch_timeout=3)
        print(f"[2단계] Consumer 실행 (배치 크기: 5, 타임아웃: 3초)...")
        
        consumer_thread = run_consumer_in_background(consumer, duration=6)
        time.sleep(5)  # 타임아웃 대기
        
        # 3. MySQL 저장 확인 (타임아웃으로 플러시되어야 함)
        final_count = get_user_events_count(self.mysql_client)
        new_events = final_count - self.initial_count
        
        print(f"[3단계] MySQL 저장 확인:")
        print(f"  - 새로 저장된 이벤트: {new_events}")
        
        # 검증: 타임아웃으로 인해 3건이 저장되어야 함
        user_count = get_user_events_count(self.mysql_client, "test-user-003")
        assert user_count >= 3, f"타임아웃으로 인해 3건이 저장되어야 함 (실제: {user_count}건)"
        
        print(f"✓ 배치 타임아웃 테스트 통과: {user_count}건 저장됨")
    
    def test_consumer_multiple_sessions(self):
        """여러 세션의 이벤트 저장 테스트"""
        from src.kafka.consumer import KafkaEventConsumer
        
        # 1. 여러 세션의 이벤트 생성
        events = []
        for session_idx in range(3):
            for event_idx in range(3):
                events.append(
                    create_test_event(
                        user_id=f"test-user-004",
                        session_id=f"test-session-{session_idx:03d}",
                        event_type="click" if event_idx == 0 else "watch",
                        content_id=f"content-{session_idx}-{event_idx}",
                        watched_minutes=5 if event_idx > 0 else 0,
                    )
                )
        
        print(f"\n[1단계] 3개 세션, 각 3건씩 총 9건 이벤트 전송...")
        send_events_to_kafka(events, self.kafka_config.topic)
        time.sleep(1)
        
        # 2. Consumer 실행
        consumer = KafkaEventConsumer(batch_size=10, batch_timeout=3)
        print(f"[2단계] Consumer 실행...")
        
        consumer_thread = run_consumer_in_background(consumer, duration=5)
        time.sleep(6)
        
        # 3. MySQL 저장 확인
        final_count = get_user_events_count(self.mysql_client)
        new_events = final_count - self.initial_count
        
        print(f"[3단계] MySQL 저장 확인:")
        print(f"  - 총 새로 저장된 이벤트: {new_events}")
        
        # 검증
        assert new_events >= 9, f"예상: 9건 이상, 실제: {new_events}건"
        
        # 세션별 이벤트 확인
        query = "SELECT session_id, COUNT(*) as cnt FROM user_events WHERE user_id = 'test-user-004' GROUP BY session_id"
        result = self.mysql_client.fetch_all(query)
        
        print(f"  - 세션별 이벤트 수:")
        for row in result:
            session_id = row['session_id']
            cnt = row['cnt']
            print(f"    {session_id}: {cnt}건")
            assert cnt >= 3, f"각 세션당 최소 3건 이상 저장되어야 함"
        
        print(f"✓ 다중 세션 테스트 통과: 총 {new_events}건 저장됨")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

