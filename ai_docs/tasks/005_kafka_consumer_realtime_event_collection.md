# Task 005: Kafka Consumer를 통한 실시간 이벤트 수집 구현

> **프로젝트:** 실시간 사용자 행동 분석 & 개인화 추천 시스템  
> **환경:** Windows Local (Cursor IDE)  
> **작성일:** 2025-01-03  
> **담당:** AI Agent

---

## 1. Task Overview

### Task Title
**Title:** Kafka Consumer를 통한 실시간 이벤트 수집 구현

### Goal Statement
**Goal:** 
Kafka 클러스터로부터 사용자 이벤트를 실시간으로 수집하고, 파싱하여 MySQL 저장을 수행하는 안정적인 Consumer 서비스를 구현합니다. 이벤트 손실을 최소화하고, 에러 핸들링 및 재시도 로직을 통해 높은 가용성을 보장합니다.

### Business Value
- **실시간 데이터 파이프라인**: Producer에서 생성된 이벤트를 즉시 수집하여 분석 및 추천 시스템에 활용
- **데이터 무결성**: 수동 커밋 방식으로 이벤트 손실 방지
- **성능 최적화**: 배치 저장으로 DB 부하 감소
- **안정성**: 자동 재연결 및 에러 핸들링으로 24/7 운영 가능

---

## 2. Current State Analysis

### Existing Infrastructure
**완료된 구성 요소:**
- ✅ Kafka 클러스터 연결 설정 (`config/kafka_config.py`)
- ✅ Redis 연결 설정 (`config/redis_config.py`)
- ✅ MySQL 연결 설정 및 클라이언트 (`config/mysql_config.py`, `src/storage/mysql_client.py`)
- ✅ Kafka Producer - 이벤트 시뮬레이터 (`src/kafka/producer.py`)
- ✅ 데이터베이스 스키마 (`user_events` 테이블 포함)
- ✅ 샘플 데이터 생성 완료

**확인된 설정값 (사용자 제공):**
```bash
# Kafka 설정
KAFKA_BOOTSTRAP_SERVERS=192.168.150.115:9092,192.168.150.120:9092,192.168.150.125:9092
KAFKA_TOPIC=user-events-topic
KAFKA_CONSUMER_GROUP=recommendation-engine-group
KAFKA_AUTO_OFFSET_RESET=earliest
KAFKA_ENABLE_AUTO_COMMIT=false

# Redis 설정
REDIS_HOST=192.168.150.115:6379
REDIS_PASSWORD=fpemdnemzpdl123$
REDIS_DB=0
REDIS_TTL=600

# MySQL 설정 (기존 설정 사용)
```

### Missing Components
**구현 필요:**
- ❌ Kafka Consumer 구현 (`src/kafka/consumer.py`)
- ❌ 단위 테스트 (`tests/test_kafka_consumer.py`)

---

## 3. Technical Requirements

### Functional Requirements

#### 3.1 Kafka Consumer 기능
- **자동 연결**: `.env` 설정을 읽어 Kafka 클러스터에 자동 연결
- **이벤트 소비**: `user-events-topic`에서 JSON 이벤트 실시간 수신
- **스키마 파싱**: Producer의 이벤트 구조와 호환되는 파싱 로직
- **수동 커밋**: 처리 완료 후에만 오프셋 커밋 (`enable_auto_commit=false`)
- **재시도 로직**: 파싱 실패 시 최대 3회 재시도
- **우아한 종료**: SIGINT/SIGTERM 시그널 처리 (Ctrl+C)

#### 3.2 MySQL 저장
- **배치 저장**: 100개 단위로 묶어서 저장 (성능 최적화)
- **트랜잭션**: 배치 저장 시 트랜잭션 사용
- **에러 처리**: 저장 실패 시 로그 기록 및 재시도

### Non-Functional Requirements

#### 성능 요구사항
- **처리량**: 초당 1,000개 이상 이벤트 처리
- **지연시간**: 이벤트 수신 후 500ms 이내 저장 완료
- **배치 간격**: 100개 도달 또는 5초 마다 자동 플러시
- **메모리 사용**: 배치 버퍼 최대 1,000개 이벤트

#### 안정성 요구사항
- **자동 재연결**: Kafka/Redis/MySQL 연결 끊김 시 30초 이내 재연결
- **이벤트 손실 방지**: 오프셋 수동 커밋으로 손실률 0%
- **에러 핸들링**: 모든 외부 연결에 try-except 적용
- **로깅**: 모든 주요 이벤트 로깅 (INFO/ERROR 레벨)

#### 운영 요구사항
- **헬스 체크**: Consumer 상태 확인 API
- **메트릭 수집**: 처리량, 지연시간, 에러율 추적
- **설정 가능**: CLI 옵션으로 배치 크기/간격 조정 가능

---

## 4. Event Schema & Data Flow

### 4.1 Input Event Schema (Kafka → Consumer)

**Producer 이벤트 구조 (ai_docs/task_template.md 기준):**
```json
{
  "event_id": "evt-123456",
  "timestamp": "2025-01-15T10:30:45.123Z",
  "user_id": "user-98765",
  "session_id": "sess-abc-def",
  "event_type": "watch",
  "content_id": "movie-555",
  "genre": "action",
  "duration_minutes": 120,
  "watched_minutes": 45,
  "metadata": {
    "user_segment": "VIP",
    "ab_test_group": "A",
    "content_type": "movie",
    "title": "액션 영화",
    "sub_genre": "thriller",
    "release_year": 2024,
    "source": "recommendation",
    "device": "mobile",
    "playback_speed": 1.0,
    "quality": "1080p"
  }
}
```

**이벤트 타입:**
- `click`: 콘텐츠 클릭
- `watch`: 콘텐츠 시청
- `watchlist`: 찜하기 추가/삭제
- `watch_complete`: 시청 완료
- `rating`: 평점 등록

### 4.2 Data Flow

```
[Kafka Cluster]
    ↓ (kafka-python consumer)
[Event Parser & Validator]
    ↓
[Batch Buffer] ← 100개 단위
    ↓
[MySQL: user_events 테이블]
    ↓
[Offset Commit] ← 성공 시에만 커밋
```

### 4.3 Database Schema

**user_events 테이블 (기존):**
```sql
CREATE TABLE IF NOT EXISTS user_events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    session_id VARCHAR(100),
    event_type ENUM('click', 'watch', 'watchlist', 'watch_complete', 'rating') NOT NULL,
    content_id VARCHAR(50),
    watched_minutes INT,
    timestamp DATETIME NOT NULL,
    metadata JSON,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_timestamp (user_id, timestamp),
    INDEX idx_content (content_id),
    INDEX idx_session (session_id)
);
```

### 4.4 MySQL Schema Only

**user_events 테이블:**
```sql
CREATE TABLE IF NOT EXISTS user_events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    session_id VARCHAR(100),
    event_type ENUM('click', 'watch', 'watchlist', 'watch_complete', 'rating') NOT NULL,
    content_id VARCHAR(50),
    watched_minutes INT,
    timestamp DATETIME NOT NULL,
    metadata JSON,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_timestamp (user_id, timestamp),
    INDEX idx_content (content_id),
    INDEX idx_session (session_id)
);
```

---

## 5. Implementation Plan

### Phase 1: Kafka Consumer 핵심 로직 구현 ✅ (60분)

**파일:** `src/kafka/consumer.py`

**구현 내용:**
- Kafka Consumer 초기화
- 이벤트 폴링 루프
- JSON 파싱 및 검증
- 배치 버퍼 관리
- 오프셋 수동 커밋

**주요 클래스/메서드:**
```python
class KafkaEventConsumer:
    def __init__(self, batch_size: int = 100, batch_timeout: int = 5)
    def connect()
    def consume_events()
    def parse_event(message) -> dict
    def validate_event(event: dict) -> bool
    def add_to_batch(event: dict)
    def flush_batch()
    def commit_offset()
    def close()
```

### Phase 2: 통합 및 에러 핸들링 ✅ (45분)

**구현 내용:**
- MySQL 저장 로직
- 재시도 로직 (exponential backoff)
- 우아한 종료 (graceful shutdown)
- 로깅 및 메트릭 수집
- CLI 인터페이스

**에러 시나리오:**
1. Kafka 연결 실패 → 30초 후 재시도
2. MySQL 저장 실패 → 다음 배치에서 재시도 (최대 3회)
3. 오프셋 커밋 실패 → 다음 폴링에서 재시도
4. 파싱 실패 → Dead Letter Queue (로그 파일) 기록

### Phase 3: 단위 테스트 작성 ✅ (45분)

**파일:** `tests/test_kafka_consumer.py`

**테스트 케이스:**
- ✅ Kafka 연결 테스트
- ✅ 이벤트 파싱 정확성 테스트
- ✅ 배치 저장 로직 테스트
- ✅ 오프셋 커밋 테스트
- ✅ 에러 핸들링 테스트 (Mock 사용)
- ✅ 우아한 종료 테스트

**테스트 도구:**
- `pytest`: 테스트 프레임워크
- `unittest.mock`: Kafka/MySQL Mock
- `kafka-python-ng` (또는 `confluent-kafka-python`): Kafka 클라이언트

**Note:** 총 12개 테스트 케이스 (TestKafkaEventConsumer 11개 + TestIntegration 1개)

---

## 6. Detailed Implementation Specifications

### 6.1 Kafka Consumer Implementation

**파일 위치:** `src/kafka/consumer.py`

**설계 원칙:**
- 수동 커밋 (이벤트 손실 방지)
- 배치 처리 (성능 최적화)
- 우아한 종료 (시그널 핸들링)
- 상세한 로깅

**코드 구조:**
```python
"""
Kafka Consumer - 실시간 이벤트 수집
- Kafka 클러스터에서 이벤트 수신
- Redis 캐싱 및 MySQL 배치 저장
- 수동 오프셋 커밋
"""
import json
import logging
import signal
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from config import get_kafka_config
from src.storage.mysql_client import MySQLClient
from src.storage.redis_client import RedisClient

logger = logging.getLogger(__name__)

class KafkaEventConsumer:
    """Kafka 이벤트 소비자"""
    
    def __init__(self, batch_size: int = 100, batch_timeout: int = 5):
        """
        생성자
        
        Args:
            batch_size: 배치 저장 단위 (기본 100개)
            batch_timeout: 배치 타임아웃 (초, 기본 5초)
        """
        self.config = get_kafka_config()
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        
        self.consumer: Optional[KafkaConsumer] = None
        self.mysql_client = MySQLClient()
        self.redis_client = RedisClient()
        
        self.batch_buffer: List[Dict[str, Any]] = []
        self.last_flush_time = time.time()
        self.running = False
        
        # 통계
        self.total_consumed = 0
        self.total_saved = 0
        self.total_errors = 0
        
        # 시그널 핸들러 등록
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def connect(self) -> None:
        """Kafka Consumer 연결"""
        pass
    
    def consume_events(self) -> None:
        """이벤트 소비 루프 (메인 로직)"""
        pass
    
    def parse_event(self, message) -> Optional[Dict[str, Any]]:
        """Kafka 메시지 파싱"""
        pass
    
    def validate_event(self, event: Dict[str, Any]) -> bool:
        """이벤트 유효성 검증"""
        pass
    
    def process_event(self, event: Dict[str, Any]) -> None:
        """이벤트 처리 (Batch Buffer)"""
        pass
    
    def add_to_batch(self, event: Dict[str, Any]) -> None:
        """배치 버퍼에 이벤트 추가"""
        pass
    
    def should_flush(self) -> bool:
        """배치 플러시 필요 여부 판단"""
        pass
    
    def flush_batch(self) -> None:
        """배치 버퍼를 MySQL에 저장"""
        pass
    
    def commit_offset(self) -> None:
        """Kafka 오프셋 수동 커밋"""
        pass
    
    def _signal_handler(self, signum, frame):
        """시그널 핸들러 (우아한 종료)"""
        pass
    
    def close(self) -> None:
        """리소스 정리"""
        pass
```

### 6.2 CLI Interface

**실행 예시:**
```bash
# 기본 실행
python src/kafka/consumer.py

# 옵션 지정 실행
python src/kafka/consumer.py --batch-size 50 --batch-timeout 10 --log-level DEBUG

# 도움말
python src/kafka/consumer.py --help
```

**CLI 옵션:**
```python
def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Kafka Consumer - 실시간 이벤트 수집")
    
    parser.add_argument("--batch-size", type=int, default=100, help="배치 저장 단위")
    parser.add_argument("--batch-timeout", type=int, default=5, help="배치 타임아웃(초)")
    parser.add_argument("--log-level", default="INFO", help="로그 레벨")
    
    return parser
```

---

## 7. Error Handling & Retry Logic

### 7.1 에러 시나리오 및 대응

| 에러 유형 | 대응 방법 | 재시도 | 로그 레벨 |
|----------|----------|--------|----------|
| Kafka 연결 실패 | 30초 후 재연결 | 무한 | ERROR |
| Kafka 메시지 파싱 실패 | Dead Letter 로그 기록 | 없음 | WARNING |
| MySQL 저장 실패 | 다음 배치에서 재시도 (최대 3회) | 3회 | ERROR |
| 오프셋 커밋 실패 | 다음 폴링에서 재시도 | 자동 | WARNING |

### 7.2 재시도 로직 (Exponential Backoff)

```python
def retry_with_backoff(func, max_retries=3, initial_delay=1):
    """
    Exponential Backoff 재시도 로직
    
    Args:
        func: 재시도할 함수
        max_retries: 최대 재시도 횟수
        initial_delay: 초기 지연 시간(초)
    """
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            delay = initial_delay * (2 ** attempt)
            logger.warning(f"Retry {attempt+1}/{max_retries} after {delay}s: {e}")
            time.sleep(delay)
```

### 7.3 Dead Letter Queue

**파일:** `logs/dead_letter_queue.log`

**형식:**
```json
{
  "timestamp": "2025-01-03T10:30:45.123Z",
  "error_type": "ParsingError",
  "raw_message": "...",
  "error_message": "Invalid JSON format"
}
```

---

## 8. Testing Strategy

### 8.1 단위 테스트 구조

**파일:** `tests/test_kafka_consumer.py`

**테스트 클래스:**
```python
import pytest
from unittest.mock import Mock, patch, MagicMock
from src.kafka.consumer import KafkaEventConsumer

class TestKafkaEventConsumer:
    """Kafka Consumer 단위 테스트"""
    
    def test_connect_success(self):
        """Kafka 연결 성공 테스트"""
        pass
    
    def test_parse_event_valid(self):
        """유효한 이벤트 파싱 테스트"""
        pass
    
    def test_parse_event_invalid(self):
        """잘못된 이벤트 파싱 테스트"""
        pass
    
    def test_batch_buffer_flush_on_size(self):
        """배치 크기 도달 시 플러시 테스트"""
        pass
    
    def test_batch_buffer_flush_on_timeout(self):
        """배치 타임아웃 시 플러시 테스트"""
        pass
    
    def test_redis_fallback_on_error(self):
        """이벤트 처리 테스트 (추후 추천 결과 캐싱 시 구현)"""
        pass
    
    def test_mysql_retry_on_error(self):
        """MySQL 저장 실패 시 재시도 테스트"""
        pass
    
    def test_offset_commit_success(self):
        """오프셋 커밋 성공 테스트"""
        pass
    
    def test_graceful_shutdown(self):
        """우아한 종료 테스트"""
        pass

class TestRedisClient:
    """Redis Client 단위 테스트 (추후 추천 결과 캐싱용으로 구현)"""
    
    def test_cache_recommendation(self):
        """추천 결과 캐싱 테스트"""
        pass
    
    def test_get_recommendations(self):
        """추천 결과 조회 테스트"""
        pass
    
    def test_health_check(self):
        """Redis 헬스 체크 테스트"""
        pass
```

### 8.2 통합 테스트

**통합 테스트 시나리오:**
1. Producer 실행 → 100개 이벤트 생성
2. Consumer 실행 → 이벤트 수집
3. MySQL 확인 → 100개 저장 확인
4. 오프셋 확인 → 커밋 확인

---

## 9. Monitoring & Metrics

### 9.1 수집할 메트릭

**성능 메트릭:**
- `kafka_messages_consumed_total`: 총 수신 이벤트 수
- `kafka_messages_saved_total`: 총 저장 이벤트 수
- `kafka_batch_flush_total`: 배치 플러시 횟수
- `kafka_processing_latency_seconds`: 처리 지연 시간

**에러 메트릭:**
- `kafka_parsing_errors_total`: 파싱 에러 수
- `mysql_save_errors_total`: MySQL 저장 에러 수

**상태 메트릭:**
- `kafka_consumer_running`: Consumer 실행 상태 (0/1)
- `kafka_batch_buffer_size`: 현재 배치 버퍼 크기

### 9.2 로깅 전략

**로그 레벨:**
- `DEBUG`: 상세 디버깅 정보 (개발 환경)
- `INFO`: 주요 이벤트 (배치 플러시, 커밋)
- `WARNING`: 경고 (Redis 연결 실패 등)
- `ERROR`: 심각한 에러 (MySQL 저장 실패)

**로그 형식:**
```
2025-01-03 10:30:45,123 INFO src.kafka.consumer - Consumed 100 events, flushing batch...
2025-01-03 10:30:45,456 INFO src.kafka.consumer - Batch saved: 100 events to MySQL
2025-01-03 10:30:45,789 INFO src.kafka.consumer - Offset committed: partition=0, offset=1234
```

---

## 10. Task Completion Tracking

### Phase 1: Kafka Consumer 구현 ✅
- [x] KafkaEventConsumer 클래스 생성
- [x] Kafka 연결 로직 구현
- [x] 이벤트 파싱 및 검증 구현
- [x] 배치 버퍼 관리 구현
- [x] 오프셋 수동 커밋 구현

### Phase 2: 통합 및 에러 핸들링 ✅
- [x] MySQL 저장 로직 구현
- [x] 재시도 로직 구현
- [x] 우아한 종료 구현
- [x] CLI 인터페이스 구현
- [x] 로깅 및 메트릭 추가

### Phase 3: 단위 테스트 ✅
- [x] KafkaEventConsumer 테스트 작성
- [x] Mock 기반 에러 테스트
- [x] 통합 테스트 시나리오 작성

### Phase 4: 통합 테스트 및 검증 ✅
- [x] Producer + Consumer 통합 테스트
- [x] 성능 테스트 (1000 events)
- [x] 에러 시나리오 테스트
- [x] 문서 업데이트

---

## 11. File Structure

```
anomaly-detection-service/
├── src/
│   ├── kafka/
│   │   ├── __init__.py
│   │   ├── producer.py          # ✅ 기존
│   │   └── consumer.py          # ✅ 구현 완료
│   └── storage/
│       ├── __init__.py
│       └── mysql_client.py      # ✅ 기존 (batch_insert 추가)
├── tests/
│   ├── test_connections.py      # ✅ 기존
│   ├── test_db_schema.py        # ✅ 기존
│   └── test_kafka_consumer.py   # ✅ 구현 완료
├── logs/
│   └── dead_letter_queue.log    # 자동 생성
└── requirements.txt             # pytest, pytest-mock 추가됨
```

---

## 12. Dependencies

### 추가 패키지

**requirements.txt에 추가:**
```
pytest>=7.4.0           # 테스트 프레임워크
pytest-mock>=3.12.0     # Mock 도구
```

**설치 명령:**
```bash
pip install pytest pytest-mock
```

**Note:** Redis 라이브러리는 추후 추천 결과 캐싱 구현 시 추가

---

## 13. Next Steps

**이 작업 완료 후:**
1. ✅ Kafka Consumer 구현 완료
2. ➡️ PySpark Streaming 구현 (세션 추적)
3. ➡️ 추천 알고리즘 구현 (협업 필터링)
4. ➡️ REST API 개발
5. ➡️ UI 대시보드 개발

---

## 14. References

- [Kafka Python Client Documentation](https://kafka-python.readthedocs.io/)
- [Redis Python Documentation](https://redis-py.readthedocs.io/)
- [task_template.md - Event Schema](../task_template.md)
- [producer.py - Event Generation Logic](../../src/kafka/producer.py)
- [mysql_client.py - Database Operations](../../src/storage/mysql_client.py)

---

## 15. Risk Assessment

### 높은 위험
- **Kafka 연결 불안정**: 네트워크 문제로 Consumer 중단 → 재연결 로직으로 완화
- **배치 저장 실패**: MySQL 부하 시 저장 실패 → 재시도 로직 및 로깅

### 중간 위험
- **오프셋 커밋 실패**: 중복 이벤트 처리 가능성 → 멱등성 설계 고려

### 낮은 위험
- **파싱 에러**: 잘못된 JSON 형식 → Dead Letter Queue로 처리
- **성능 저하**: 배치 크기 조정으로 해결 가능

---

**작성 완료일:** 2025-01-03  
**예상 구현 시간:** 3-4시간  
**우선순위:** 높음 (Phase 2 필수 구성 요소)



