# Task 006: PySpark Streaming 기본 설정 및 Kafka 연동

> **프로젝트:** 실시간 사용자 행동 분석 & 개인화 추천 시스템  
> **환경:** Windows Local (Cursor IDE)  
> **작성일:** 2026-01-04  
> **담당:** AI Agent

---

## 1. Task Overview

### Task Title
**Title:** PySpark Streaming 기본 설정 및 Kafka 연동

### Goal Statement
**Goal:** 
PySpark Streaming을 사용하여 Kafka로부터 실시간 사용자 이벤트를 읽고, JSON 파싱 및 스키마 검증을 수행하는 기본 스트리밍 파이프라인을 구축합니다. 체크포인트를 통한 장애 복구 지원과 3초 배치 간격으로 안정적인 스트림 처리를 구현합니다.

### Business Value
- **실시간 스트림 처리**: Kafka 이벤트를 PySpark로 실시간 처리하여 세션 추적 및 분석 기반 마련
- **장애 복구**: 체크포인트를 통한 자동 복구로 데이터 손실 방지
- **확장성**: Spark의 분산 처리 능력으로 향후 대용량 처리 대비
- **표준화**: Spark Structured Streaming 표준 패턴 적용

---

## 2. Current State Analysis

### Existing Infrastructure
**완료된 구성 요소:**
- ✅ Kafka 클러스터 연결 설정 (`config/kafka_config.py`)
- ✅ Spark 설정 클래스 (`config/spark_config.py`)
- ✅ Kafka Producer - 이벤트 시뮬레이터 (`src/kafka/producer.py`)
- ✅ Kafka Consumer - 실시간 수집 (`src/kafka/consumer.py`)
- ✅ 데이터베이스 스키마 (`user_events` 테이블 포함)
- ✅ 샘플 데이터 생성 완료

**확인된 설정값:**
```bash
# Spark 설정 (.env)
SPARK_APP_NAME=RecommendationSystem
SPARK_MASTER=local[*]
SPARK_EXECUTOR_MEMORY=2g
SPARK_DRIVER_MEMORY=1g
SPARK_LOG_LEVEL=WARN

# Kafka 설정 (.env)
KAFKA_BOOTSTRAP_SERVERS=192.168.150.115:9092,192.168.150.120:9092,192.168.150.125:9092
KAFKA_TOPIC=user-events-topic
```

### Current State
- **Kafka Producer**: 이벤트 생성 및 전송 완료
- **Kafka Consumer**: Python 기반 Consumer로 MySQL 저장 완료
- **PySpark Streaming**: 미구현 (이번 작업에서 구현)

---

## 3. Project Objectives & System Design

### System Purpose
PySpark Structured Streaming을 통해 Kafka 이벤트를 실시간으로 처리하고, 향후 세션 추적 및 추천 알고리즘 적용을 위한 기반을 마련합니다.

### Core Objectives
1. **Kafka 소스 연결**: Spark Streaming에서 Kafka 토픽 읽기
2. **JSON 파싱**: Kafka 메시지의 JSON 파싱 및 스키마 검증
3. **체크포인트 관리**: 장애 복구를 위한 체크포인트 저장
4. **기본 출력**: 콘솔 출력으로 스트림 처리 검증

### Target Event Schema
**UserEvent 데이터 모델** (task_template.md 및 producer.py 기준):
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
    "title": "Movie Title",
    "sub_genre": "action",
    "release_year": 2020,
    "source": "home",
    "device": "mobile"
  }
}
```

### Expected System Benefits
- **실시간 처리**: 3초 배치 간격으로 이벤트 실시간 처리
- **장애 복구**: 체크포인트를 통한 자동 복구
- **확장성**: 향후 세션 추적, 집계, ML 파이프라인 확장 가능
- **표준화**: Spark Structured Streaming 표준 패턴

---

## 4. Development Mode Context

- **🚨 Project Stage:** 신규 개발 (MVP 구축 단계)
- **Breaking Changes:** 허용 (초기 개발 단계)
- **Data Handling:** 테스트 데이터 사용
- **User Base:** 개발자 본인 (로컬 테스트 환경)
- **Priority:** 기능 구현 속도 > 완벽한 안정성

---

## 5. Technical Requirements

### Functional Requirements
**시스템 기능:**
- PySpark 세션을 `.env`의 Spark 설정으로 초기화
- Kafka 토픽 `user-events-topic`에서 스트림 읽기
- Consumer Group: `spark-streaming-group` (별도 그룹)
- Starting Offset: `latest`
- JSON 메시지 파싱 및 스키마 검증
- 체크포인트 위치: `data/checkpoints/streaming/`
- 배치 간격: 3초 (task_template.md 명세)
- 콘솔 출력 (테스트용)

### Non-Functional Requirements
- **Performance:** 
  - 배치 간격: 3초
  - 이벤트 처리 지연시간 < 500ms
- **Reliability:** 
  - 체크포인트를 통한 장애 복구
  - 스키마 검증 실패 시 에러 로깅
- **Maintainability:** 
  - 설정은 `.env`에서 관리
  - 명확한 로깅 및 에러 메시지

### Technical Constraints
- PySpark는 로컬에서만 실행 가능 (Standalone 모드)
- Schema Registry 미사용 (순수 JSON 파싱)
- Windows 환경에서 실행 가능해야 함

---

## 6. Data & Database Changes

### Data Model
**PySpark StructType 스키마:**
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, MapType

user_event_schema = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("timestamp", StringType(), nullable=False),  # ISO 8601 문자열
    StructField("user_id", StringType(), nullable=False),
    StructField("session_id", StringType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("content_id", StringType(), nullable=True),
    StructField("genre", StringType(), nullable=True),
    StructField("duration_minutes", IntegerType(), nullable=True),
    StructField("watched_minutes", IntegerType(), nullable=True),
    StructField("metadata", MapType(StringType(), StringType()), nullable=True)
])
```

### Data Migration Plan
- 체크포인트 디렉토리 생성: `data/checkpoints/streaming/`
- 기존 데이터 마이그레이션 불필요 (신규 스트림 처리)

---

## 7. Implementation Plan

### Phase 1: 기본 구조 설정
**Tasks:**
- [x] `src/spark/streaming.py` 파일 생성
- [x] PySpark 세션 초기화 함수 구현
- [x] Spark 설정 로드 (`config/spark_config.py` 사용)
- [x] 체크포인트 디렉토리 생성 로직

**Files:**
- `src/spark/streaming.py`

### Phase 2: Kafka 소스 연결
**Tasks:**
- [x] Kafka 소스 설정 (bootstrap servers, topic)
- [x] Consumer Group 설정 (`spark-streaming-group`)
- [x] Starting Offset 설정 (`latest`)
- [x] Kafka 설정 로드 (`config/kafka_config.py` 사용)

### Phase 3: 스키마 정의 및 파싱
**Tasks:**
- [x] UserEvent StructType 스키마 정의
- [x] JSON 파싱 로직 구현
- [x] 스키마 검증 로직
- [x] 에러 핸들링 (파싱 실패 시)

### Phase 4: 스트림 처리 파이프라인
**Tasks:**
- [x] 스트림 읽기 (Kafka 소스)
- [x] JSON 파싱 및 스키마 적용
- [x] 스키마 검증
- [x] 콘솔 출력 (테스트용)
- [x] 체크포인트 설정

### Phase 5: 실행 및 테스트
**Tasks:**
- [x] 스트리밍 작업 시작 로직
- [x] 우아한 종료 (Graceful Shutdown)
- [x] CLI 인터페이스
- [x] README에 실행 방법 추가

---

## 8. File Structure & Organization

```
src/spark/
├── __init__.py
└── streaming.py          # PySpark Streaming 메인 로직

data/
└── checkpoints/
    └── streaming/        # 체크포인트 저장 위치
```

---

## 9. Implementation Details

### Spark 세션 초기화
```python
from config import get_spark_config
from pyspark.sql import SparkSession

def create_spark_session() -> SparkSession:
    """Spark 세션 생성"""
    config = get_spark_config()
    spark = SparkSession.builder \
        .appName(config.app_name) \
        .master(config.master) \
        .config("spark.executor.memory", config.executor_memory) \
        .config("spark.driver.memory", config.driver_memory) \
        .config("spark.sql.streaming.checkpointLocation", "data/checkpoints/streaming/") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel(config.log_level)
    return spark
```

### Kafka 소스 설정
```python
from config import get_kafka_config

def create_kafka_stream(spark: SparkSession):
    """Kafka 스트림 생성"""
    kafka_config = get_kafka_config()
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config.get_bootstrap_servers_str()) \
        .option("subscribe", "user-events-topic") \
        .option("startingOffsets", "latest") \
        .option("kafka.group.id", "spark-streaming-group") \
        .load()
    
    return df
```

### JSON 파싱 및 스키마 적용
```python
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType

def parse_json_stream(df):
    """JSON 파싱 및 스키마 적용"""
    schema = get_user_event_schema()
    
    # value 컬럼을 JSON으로 파싱
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    return parsed_df
```

### 스트림 처리 및 출력
```python
def process_stream(parsed_df):
    """스트림 처리 및 콘솔 출력"""
    query = parsed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="3 seconds") \
        .start()
    
    return query
```

---

## 10. Task Completion Tracking

### Real-Time Progress Tracking
**진행 상황:**
- [x] Phase 1: 기본 구조 설정
- [x] Phase 2: Kafka 소스 연결
- [x] Phase 3: 스키마 정의 및 파싱
- [x] Phase 4: 스트림 처리 파이프라인
- [x] Phase 5: 실행 및 테스트

**완료된 파일:**
- `src/spark/streaming.py` ✅
- `README.md` (PySpark 실행 방법 추가) ✅

---

## 11. Testing & Validation

### 테스트 시나리오
1. **기본 실행 테스트**
   ```bash
   python src/spark/streaming.py
   ```
   - Spark 세션 정상 생성 확인
   - Kafka 연결 확인
   - 스트림 읽기 확인

2. **이벤트 처리 테스트**
   - Producer 실행하여 이벤트 생성
   - Streaming에서 이벤트 수신 확인
   - JSON 파싱 정상 동작 확인
   - 콘솔 출력 확인

3. **체크포인트 테스트**
   - 스트리밍 중단 후 재시작
   - 체크포인트에서 복구 확인
   - 중복 처리 없음 확인

### 성공 기준
- ✅ Spark 세션 정상 생성
- ✅ Kafka 토픽에서 스트림 읽기 성공
- ✅ JSON 파싱 및 스키마 검증 성공
- ✅ 콘솔 출력 정상 동작
- ✅ 체크포인트 저장 및 복구 동작
- ✅ 3초 배치 간격 정상 동작

---

## 12. Next Steps

### 향후 작업
- [ ] Task 007: 세션 추적 로직 구현 (3분 윈도우)
- [ ] Task 008: 이벤트 집계 및 통계 계산
- [ ] Task 009: MySQL 저장 로직 추가
- [ ] Task 010: 추천 알고리즘 통합

---

## 13. Notes & Considerations

### 주의사항
- **체크포인트 디렉토리**: `data/checkpoints/streaming/` 디렉토리가 존재해야 함
- **Kafka Consumer Group**: 기존 `recommendation-engine-group`과 별도로 `spark-streaming-group` 사용
- **Starting Offset**: `latest`로 설정하여 기존 메시지는 처리하지 않음 (테스트 목적)
- **Windows 환경**: PySpark는 Windows에서 실행 시 추가 설정 필요할 수 있음

### 성능 고려사항
- 배치 간격 3초는 task_template.md 명세에 따라 설정
- 향후 처리량에 따라 조정 가능
- 체크포인트는 디스크 I/O를 유발하므로 SSD 권장

---

**Last Updated**: 2026-01-06  
**Status**: ✅ 완료

## 14. Post-Implementation Updates

### Windows 환경 호환성 개선 (2026-01-06)
- **Python Worker 연결 문제 해결**: Windows 환경에서 Python Worker 연결 실패 문제 해결
  - `spark.python.worker.reuse=false`: Worker 재사용 비활성화
  - `spark.python.use.daemon=false`: Daemon 비활성화
  - `spark.pyspark.python`: 현재 Python 인터프리터 경로 명시
  - `spark.driver.host=localhost`: localhost 명시
  - 메모리 설정: 4g로 증가 (안정성 향상)

### 체크포인트 기반 오프셋 관리
- Spark Structured Streaming은 체크포인트를 통해 오프셋을 자동 관리합니다
- Consumer Group을 사용하지 않습니다
- Starting Offset은 체크포인트가 없으면 `earliest`, 있으면 `latest`로 자동 설정됩니다


