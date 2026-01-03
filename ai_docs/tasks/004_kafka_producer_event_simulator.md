# Task 004: Kafka Producer 구현 (이벤트 시뮬레이터)

> **작업 번호:** 004  
> **작업명:** Kafka Producer를 통한 사용자 이벤트 시뮬레이터 구현  
> **의존성:** Task 001 (Configuration Files Setup), Task 003 (Sample Data Generation)  
> **상태:** 진행 중  
> **생성일:** 2026-01-01

---

## 1. Task Overview

### Task Title
**Title:** Kafka Producer를 통한 사용자 이벤트 시뮬레이터 구현

### Goal Statement
**Goal:**  
샘플 사용자/콘텐츠 데이터를 기반으로 현실적인 사용자 행동 이벤트를 생성하여 Kafka 토픽(`user-events-topic`)으로 전송하는 이벤트 시뮬레이터를 구현합니다.

**비즈니스 가치:**
- Kafka → Streaming → Storage 파이프라인의 **엔드투엔드 검증** 가능
- 추천/세션 분석 로직 개발 시 **반복 가능한 테스트 입력** 제공
- 실제 트래픽과 유사한 이벤트 패턴으로 **성능/안정성 점검** 가능

---

## 2. Current State Analysis

### 기존 상태
- ✅ Kafka 설정 클래스 구현 완료 (`config/kafka_config.py`)
- ✅ 샘플 데이터 존재
  - `data/users.json` (사용자 100명)
  - `data/contents.json` (콘텐츠 1,000개: 영화/드라마/다큐)
- ❌ Kafka Producer 코드 없음 (`src/kafka/producer.py` 미구현)
- ❌ 이벤트 시뮬레이션 실행 커맨드/옵션 정의 없음

---

## 3. Technical Requirements

### 3.1 Functional Requirements
- `.env`의 Kafka 설정을 사용하여 Kafka Producer가 동작한다
  - **Bootstrap Servers:** `192.168.150.115:9092,192.168.150.120:9092,192.168.150.125:9092`
  - **Topic:** `user-events-topic`
- `data/users.json`에서 사용자 정보를 로드하여 이벤트 생성에 사용한다
- `data/contents.json`에서 콘텐츠 정보를 로드하여 이벤트 생성에 사용한다
- 랜덤하게 다양한 사용자 행동을 생성한다 (click/watch/watch_complete/watchlist/rating)
- 현실적인 시간 간격으로 이벤트를 발생시킨다 (짧은 버스트 + 랜덤 지연)

### 3.2 Non-Functional Requirements
- **재현 가능성:** 동일 시드(seed) 사용 시 유사한 이벤트 패턴 재현 가능
- **운영 편의성:** CLI 옵션으로 이벤트 수/간격/드라이런/로그 레벨 조절 가능
- **안정성:** Kafka 장애/전송 실패 시 명확한 로그와 안전한 종료(또는 선택적 재시도)

---

## 4. Event Schema (Reference)

`ai_docs/task_template.md`의 데이터 모델을 기준으로 Kafka 메시지(JSON)를 구성합니다.

```python
class EventType(Enum):
    """이벤트 타입"""
    CLICK = "click"
    WATCH = "watch"
    WATCHLIST = "watchlist"
    WATCH_COMPLETE = "watch_complete"
    RATING = "rating"

class ContentType(Enum):
    """콘텐츠 타입"""
    MOVIE = "movie"
    SERIES = "series"
    DOCUMENTARY = "documentary"

@dataclass
class UserEvent:
    """Kafka로부터 수신한 사용자 이벤트"""
    event_id: str
    timestamp: datetime
    user_id: str
    session_id: str
    event_type: EventType
    content_id: str
    genre: str
    duration_minutes: int
    watched_minutes: int
    metadata: Dict[str, Any]
```

### 메시지 직렬화 규칙
- Kafka 전송 값(value)은 **JSON 문자열(UTF-8)** 로 전송한다 (Schema Registry 미사용)
- `timestamp`는 **ISO 8601 문자열**로 직렬화한다 (예: `2026-01-01T12:34:56.789Z`)
- `event_type`는 Enum 값의 **string**(`"click"`, `"watch"`, …)으로 전송한다

---

## 5. Input Data Specs

### 5.1 Users (`data/users.json`)
- 리스트 형태의 JSON
- 주요 필드:
  - `user_id`, `user_segment`, `signup_date`, `total_purchases`, `total_spent`, `favorite_categories`
- 참고: `favorite_categories`가 JSON 문자열 형태로 저장된 케이스가 있어 안전 파싱 필요

### 5.2 Contents (`data/contents.json`)
- 리스트 형태의 JSON
- 주요 필드:
  - `content_id`, `title`, `content_type`, `genre`, `sub_genre`, `duration_minutes`, `release_year`, `rating`, `review_count`

---

## 6. Simulation Logic Design

### 6.1 이벤트 타입별 생성 규칙(예시)
- **CLICK**
  - `watched_minutes = 0`
  - `metadata`: `{"source": "home|search|recommendation", "device": "..."}`
- **WATCH**
  - `watched_minutes`: \(1 \sim duration\) 사이의 현실적 분포(짧게 시작하는 경우 다수)
  - `metadata`: `{"playback_speed": 1.0, "quality": "720p|1080p"}`
- **WATCH_COMPLETE**
  - `watched_minutes = duration_minutes`
  - `metadata`: `{"completion": 1.0}`
- **WATCHLIST**
  - `watched_minutes = 0`
  - `metadata`: `{"action": "add"}`
- **RATING**
  - `watched_minutes`: 0 또는 최근 시청 분량 기반
  - `metadata`: `{"rating": 1~5, "review": "optional"}`

### 6.2 세션(session) 모델
- `session_id`는 사용자별로 유지하며, **랜덤 이벤트 시퀀스**를 세션 단위로 묶는다
- 일정 시간 이상 갭이 발생하면 새 세션을 생성한다 (예: 3~10분 랜덤)

### 6.3 시간 간격(현실성)
- 이벤트 간 sleep은 `min_interval_ms ~ max_interval_ms` 범위의 랜덤 값
- 사용자가 옵션으로 TPS(초당 이벤트) 또는 간격 범위를 조절할 수 있어야 한다

---

## 7. Implementation Details

### 파일 구조
```
src/
└── kafka/
    ├── __init__.py
    └── producer.py   # ✅ 이번 작업에서 구현
```

### 주요 기능
- `load_users(path)`, `load_contents(path)` 로 샘플 데이터 로드
- `generate_event(user, content, event_type, session_id, now)` 로 이벤트 생성
- `KafkaProducer`를 사용하여 토픽으로 전송
- CLI 지원:
  - `--events N` (총 전송 이벤트 수, 기본: 무한/또는 큰 값)
  - `--min-interval-ms`, `--max-interval-ms`
  - `--dry-run` (Kafka 전송 없이 stdout 로그만)
  - `--seed` (재현용)
  - `--log-level`

---

## 8. Success Criteria

- ✅ `src/kafka/producer.py` 구현 완료
- ✅ `.env` Kafka 설정 기반으로 Producer가 토픽에 메시지를 전송한다
- ✅ 샘플 데이터 기반 이벤트 생성이 동작한다 (users/contents 로딩 포함)
- ✅ 이벤트 스키마 필드가 누락 없이 생성된다 (`event_id`, `timestamp`, `user_id`, …, `metadata`)
- ✅ 드라이런 모드에서 정상적으로 이벤트가 출력된다

---

## 9. Execution Commands

```bash
# (프로젝트 루트에서)

# 1) 드라이런(전송 없음) - 이벤트 10개만 생성/출력
python src/kafka/producer.py --dry-run --events 10

# 2) 실제 Kafka로 전송 - 이벤트 100개
python src/kafka/producer.py --events 100

# 3) 이벤트 간격 조절 (100~800ms)
python src/kafka/producer.py --events 200 --min-interval-ms 100 --max-interval-ms 800

# 4) 재현 가능한 시뮬레이션
python src/kafka/producer.py --events 50 --seed 42
```

---

## 10. Next Steps

이 작업 완료 후:
1. Kafka Consumer 구현 (`src/kafka/consumer.py`) 및 이벤트 수신 검증
2. 수신 이벤트를 MySQL에 저장하는 적재 로직 구현
3. PySpark 스트리밍(세션화/집계)과 연결

---

**작업 시작일:** 2026-01-01  
**예상 완료일:** 2026-01-01  
**담당자:** AI Agent  
**상태:** 🚀 진행 중



