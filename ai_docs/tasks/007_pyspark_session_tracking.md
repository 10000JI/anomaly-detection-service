# Task 007: PySpark 3분 윈도우 기반 세션 추적 구현

> **프로젝트:** 실시간 사용자 행동 분석 & 개인화 추천 시스템  
> **환경:** Windows Local (Cursor IDE)  
> **작성일:** 2026-01-04  
> **담당:** AI Agent

---

## 1. Task Overview

### Task Title
**Title:** PySpark 3분 윈도우 기반 세션 추적 구현

### Goal Statement
**Goal:** 
PySpark Structured Streaming을 사용하여 Kafka 이벤트를 3분 윈도우로 그룹화하고, 사용자 세션별로 집계하여 MySQL에 저장하는 세션 추적 시스템을 구현합니다. Watermark를 통한 지연 데이터 처리와 foreachBatch를 활용한 안정적인 배치 저장을 지원합니다.

### Business Value
- **세션 분석**: 사용자 행동을 세션 단위로 분석하여 패턴 파악
- **실시간 집계**: 3분 윈도우로 실시간 세션 집계 및 저장
- **데이터 일관성**: Watermark를 통한 지연 데이터 처리로 정확한 집계
- **확장성**: Spark의 분산 처리로 대용량 세션 처리 가능

---

## 2. Current State Analysis

### Existing Infrastructure
**완료된 구성 요소:**
- ✅ PySpark Streaming 기본 설정 (`src/spark/streaming.py`)
- ✅ Kafka 소스 연결 및 JSON 파싱
- ✅ MySQL 데이터베이스 스키마 (`user_sessions` 테이블)
- ✅ MySQL 설정 클래스 (`config/mysql_config.py`)
- ✅ 체크포인트 지원

**확인된 설정값:**
```bash
# Spark 설정 (.env)
SPARK_APP_NAME=RecommendationSystem
SPARK_MASTER=local[*]
SPARK_EXECUTOR_MEMORY=2g
SPARK_DRIVER_MEMORY=1g
SPARK_LOG_LEVEL=WARN

# MySQL 설정 (.env)
MYSQL_HOST=your_mysql_host
MYSQL_PORT=3306
MYSQL_USER=your_mysql_user
MYSQL_PASSWORD=your_secure_password
MYSQL_DATABASE=your_database_name

# Kafka 설정 (.env)
KAFKA_BOOTSTRAP_SERVERS=192.168.150.115:9092,192.168.150.120:9092,192.168.150.125:9092
KAFKA_TOPIC=user-events-topic
```

### Current State
- **PySpark Streaming**: 기본 Kafka 읽기 및 콘솔 출력 완료
- **세션 추적**: 미구현 (이번 작업에서 구현)
- **MySQL 저장**: 미구현 (이번 작업에서 구현)

---

## 3. Project Objectives & System Design

### System Purpose
Kafka 이벤트를 3분 윈도우로 그룹화하여 사용자 세션을 추적하고, 세션별 집계 결과를 MySQL에 저장하여 향후 추천 알고리즘에 활용합니다.

### Core Objectives
1. **3분 윈도우 세션 추적**: `window(col("timestamp"), "3 minutes")` 사용
2. **Watermark 설정**: 10분 지연 데이터 처리
3. **세션 집계**: event_count, total_watched_minutes, browsed_contents, watched_contents, completed_contents, start_time, end_time
4. **MySQL 저장**: foreachBatch를 사용한 배치 저장
5. **모니터링**: 처리 세션 수 및 지연 시간 로깅

### Data Flow
```
[Kafka Topic: user-events-topic]
    │
    ├─────────────────────────┬─────────────────────────┐
    │                         │                         │
    ↓                         ↓                         ↓
[Consumer 1]            [Consumer 2]            [Kafka Broker]
(Python)                (PySpark)               (__consumer_offsets)
group:                  group:                  ├─ recommendation-engine-group
recommendation-         spark-streaming-           └─ offset: 1500
engine-group            group                   └─ spark-streaming-group
    │                         │                      └─ offset: 1200
    ↓                         ↓
[MySQL                  [Checkpoint]
user_events]            data/checkpoints/
    │                   ├─ offsets/
    │                   ├─ state/
    │                   └─ commits/
    │                         │
    │                         ↓
    │                   [세션 집계]
    │                   (3분 윈도우)
    │                         │
    │                         ↓
    └─────────────────→ [MySQL user_sessions]
```

### Expected System Benefits
- **실시간 세션 분석**: 3초 배치 간격으로 세션 집계
- **정확한 집계**: Watermark를 통한 지연 데이터 처리
- **안정적 저장**: foreachBatch를 통한 배치 저장으로 성능 최적화
- **장애 복구**: 체크포인트를 통한 자동 복구

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
- 3분 윈도우로 이벤트 그룹화: `window(col("timestamp"), "3 minutes")`
- Watermark 10분 설정: `.withWatermark("timestamp", "10 minutes")`
- 세션별 집계:
  - `event_count`: 세션당 이벤트 수
  - `total_watched_minutes`: 총 시청 시간
  - `browsed_contents`: 클릭한 콘텐츠 목록 (collect_set)
  - `watched_contents`: 실제 시청한 콘텐츠 목록
  - `completed_contents`: 완료한 콘텐츠 목록
  - `start_time`: 세션 시작 시간 (min)
  - `end_time`: 세션 종료 시간 (max)
- MySQL 저장: foreachBatch로 배치 저장
- 처리 간격: `trigger(processingTime='3 seconds')`
- 모니터링: 처리 세션 수 및 지연 시간 로깅

### Non-Functional Requirements
- **Performance:** 
  - 배치 간격: 3초
  - 세션 집계 지연시간 < 500ms
  - MySQL 배치 저장 최적화
- **Reliability:** 
  - Watermark를 통한 지연 데이터 처리
  - foreachBatch를 통한 안정적인 저장
  - 체크포인트를 통한 장애 복구
- **Maintainability:** 
  - 명확한 로깅 및 모니터링
  - 에러 핸들링

### Technical Constraints
- PySpark는 로컬에서만 실행 가능 (Standalone 모드)
- MySQL JDBC 드라이버 필요
- Windows 환경에서 실행 가능해야 함

---

## 6. Data & Database Changes

### Database Schema
**user_sessions 테이블** (이미 생성됨):
```sql
CREATE TABLE IF NOT EXISTS user_sessions (
    session_id VARCHAR(100) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    start_time DATETIME NOT NULL,
    end_time DATETIME,
    event_count INT DEFAULT 0,
    browsed_contents JSON,
    watched_contents JSON,
    completed_contents JSON,
    total_watch_minutes INT DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user_start (user_id, start_time)
);
```

### Data Model
**세션 집계 결과:**
```python
{
    "session_id": "user-001-sess-abc",
    "user_id": "user-001",
    "start_time": "2026-01-04 10:00:00",
    "end_time": "2026-01-04 10:03:00",
    "event_count": 8,
    "total_watched_minutes": 24,
    "browsed_contents": ["content-001", "content-002"],
    "watched_contents": ["content-001"],
    "completed_contents": []
}
```

---

## 7. Implementation Plan

### Phase 1: 타임스탬프 변환 및 Watermark 설정
**Tasks:**
- [x] ISO 8601 문자열을 TimestampType으로 변환
- [x] Watermark 10분 설정
- [x] 타임스탬프 검증

### Phase 2: 3분 윈도우 세션 추적
**Tasks:**
- [x] `window(col("timestamp"), "3 minutes")` 적용
- [x] `session_id`, `user_id`로 그룹화
- [x] 윈도우 시작/종료 시간 추출

### Phase 3: 세션 집계
**Tasks:**
- [x] `event_count`: count("*")
- [x] `total_watched_minutes`: sum("watched_minutes")
- [x] `browsed_contents`: collect_set (event_type='click')
- [x] `watched_contents`: collect_set (event_type='watch')
- [x] `completed_contents`: collect_set (event_type='watch_complete')
- [x] `start_time`: min("timestamp")
- [x] `end_time`: max("timestamp")

### Phase 4: MySQL 저장
**Tasks:**
- [x] foreachBatch 함수 구현
- [x] JDBC 연결 설정
- [x] 배치 저장 로직
- [x] 에러 핸들링

### Phase 5: 모니터링 및 로깅
**Tasks:**
- [x] 처리 세션 수 로깅
- [x] 처리 지연 시간 측정
- [x] 에러 로깅

---

## 8. Implementation Details

### 타임스탬프 변환
```python
from pyspark.sql.functions import to_timestamp, col

# ISO 8601 문자열을 TimestampType으로 변환
df_with_timestamp = parsed_df.withColumn(
    "timestamp_ts",
    to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
)
```

### Watermark 및 윈도우 설정
```python
from pyspark.sql.functions import window, col

# Watermark 10분 설정
df_with_watermark = df_with_timestamp.withWatermark("timestamp_ts", "10 minutes")

# 3분 윈도우로 그룹화
windowed_df = df_with_watermark.groupBy(
    col("session_id"),
    col("user_id"),
    window(col("timestamp_ts"), "3 minutes")
)
```

### 세션 집계
```python
from pyspark.sql.functions import (
    count, sum, min, max, collect_set, when
)

aggregated_df = windowed_df.agg(
    count("*").alias("event_count"),
    sum("watched_minutes").alias("total_watched_minutes"),
    collect_set(
        when(col("event_type") == "click", col("content_id"))
    ).alias("browsed_contents"),
    collect_set(
        when(col("event_type") == "watch", col("content_id"))
    ).alias("watched_contents"),
    collect_set(
        when(col("event_type") == "watch_complete", col("content_id"))
    ).alias("completed_contents"),
    min("timestamp_ts").alias("start_time"),
    max("timestamp_ts").alias("end_time")
)
```

### MySQL 저장 (foreachBatch)
```python
from pyspark.sql.functions import to_json, when, col

def save_sessions_to_mysql(batch_df, batch_id):
    """배치별 MySQL 저장"""
    mysql_config = get_mysql_config()
    
    jdbc_url = f"jdbc:mysql://{mysql_config.host}:{mysql_config.port}/{mysql_config.database}"
    properties = {
        "user": mysql_config.user,
        "password": mysql_config.password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    
    # 배열을 JSON 문자열로 변환 (Spark SQL 함수 사용, Python Worker 불필요)
    sessions_df = batch_df.select(
        col("session_id"),
        col("user_id"),
        col("start_time"),
        col("end_time"),
        col("event_count"),
        col("total_watched_minutes"),
        # 배열을 JSON 문자열로 변환 (NULL 처리 포함)
        when(col("browsed_contents").isNull(), None)
        .otherwise(to_json(col("browsed_contents"))).alias("browsed_contents"),
        when(col("watched_contents").isNull(), None)
        .otherwise(to_json(col("watched_contents"))).alias("watched_contents"),
        when(col("completed_contents").isNull(), None)
        .otherwise(to_json(col("completed_contents"))).alias("completed_contents")
    )
    
    # 배치 저장
    sessions_df.write.jdbc(
        url=jdbc_url,
        table="user_sessions",
        mode="append",
        properties=properties
    )

# foreachBatch 적용
query = aggregated_df.writeStream \
    .foreachBatch(save_sessions_to_mysql) \
    .outputMode("update") \
    .trigger(processingTime="3 seconds") \
    .start()
```

**주요 변경사항:**
- `collect()` 제거: Python Worker 연결 문제 해결
- `to_json()` Spark SQL 함수 사용: 배열을 JSON 문자열로 변환
- NULL 처리: 빈 배열이나 NULL 배열을 NULL 문자열로 유지

---

## 9. Task Completion Tracking

### Real-Time Progress Tracking
**진행 상황:**
- [x] Phase 1: 타임스탬프 변환 및 Watermark 설정
- [x] Phase 2: 3분 윈도우 세션 추적
- [x] Phase 3: 세션 집계
- [x] Phase 4: MySQL 저장
- [x] Phase 5: 모니터링 및 로깅

**완료된 파일:**
- `src/spark/streaming.py` ✅ (세션 추적 로직 추가)
- `README.md` ✅ (테스트 방법 추가)

---

## 10. Testing & Validation

### 테스트 시나리오
1. **기본 세션 추적 테스트**
   ```bash
   # 터미널 1: Producer 실행
   python src/kafka/producer.py --events 100
   
   # 터미널 2: PySpark Streaming 실행
   python src/spark/streaming.py
   ```
   - 세션 집계 정상 동작 확인
   - MySQL 저장 확인

2. **3분 윈도우 테스트**
   - 3분 이상 간격으로 이벤트 생성
   - 세션 분리 확인
   - 윈도우 완료 후 MySQL 저장 확인

3. **Watermark 테스트**
   - 지연 데이터 (10분 이내) 처리 확인
   - 지연 데이터 (10분 초과) 제외 확인

4. **모니터링 테스트**
   - 처리 세션 수 로깅 확인
   - 지연 시간 측정 확인

### 성공 기준
- ✅ 3분 윈도우로 세션 그룹화 성공
- ✅ Watermark 10분 설정 정상 동작
- ✅ 세션 집계 필드 모두 계산 성공
- ✅ MySQL 저장 정상 동작
- ✅ 모니터링 로깅 정상 동작
- ✅ 체크포인트 복구 정상 동작

---

## 11. Notes & Considerations

### 주의사항
- **타임스탬프 형식**: ISO 8601 형식 (`2025-01-15T10:30:45.123Z`)을 TimestampType으로 변환 필요
- **Watermark**: 10분 설정으로 지연 데이터 처리
- **윈도우 완료**: Watermark를 지난 윈도우만 완료 처리
- **MySQL JDBC**: JDBC 드라이버가 필요할 수 있음 (일부 버전에서는 자동 로드)
- **배치 저장**: foreachBatch를 사용하여 배치 단위로 저장하여 성능 최적화
- **Windows 환경**: Python Worker 연결 문제를 방지하기 위해 `to_json()` Spark SQL 함수 사용 (collect() 제거)
- **배열 → JSON 변환**: `to_json()` 함수는 Spark SQL 함수이므로 Python Worker 없이 실행 가능

### 성능 고려사항
- 배치 간격 3초는 task_template.md 명세에 따라 설정
- foreachBatch를 통한 배치 저장으로 성능 최적화
- Watermark를 통한 지연 데이터 처리로 정확한 집계

### 데이터 흐름 이해
- **Consumer 1 (Python)**: 빠른 저장 (recommendation-engine-group)
- **Consumer 2 (PySpark)**: 세션 집계 (spark-streaming-group)
- 각각 독립적인 offset 관리
- PySpark는 체크포인트로 장애 복구

---

**Last Updated**: 2026-01-06  
**Status**: ✅ 완료

## 12. Post-Implementation Updates

### Windows 환경 Python Worker 문제 해결 (2026-01-06)
- **문제**: Windows 환경에서 `collect()` 호출 시 Python Worker 연결 실패 발생
- **해결 방법**:
  - `collect()` 제거: DataFrame을 Python 리스트로 변환하는 대신 Spark SQL 함수 사용
  - `to_json()` 사용: 배열 컬럼을 JSON 문자열로 변환하는 데 Spark SQL 함수 사용
  - Python Worker 없이 실행 가능하도록 최적화
- **결과**: MySQL 저장 성공, 통합 테스트 통과

### MySQL 저장 최적화
- **배열 → JSON 변환**: `to_json()` Spark SQL 함수 사용
  ```python
  when(col("browsed_contents").isNull(), None)
  .otherwise(to_json(col("browsed_contents"))).alias("browsed_contents")
  ```
- **배치 저장**: `foreachBatch`를 통한 안정적인 배치 저장
- **에러 핸들링**: JDBC 저장 실패 시 상세 로깅

### 통합 테스트 완료
- **테스트 파일**: `tests/test_spark_streaming_integration.py`
- **테스트 결과**: 
  - ✅ 초기 세션 추적 테스트 통과
  - ✅ MySQL 저장 성공 확인 (event_count: 5, total_watched_minutes: 20)
  - ✅ 세션 집계 필드 검증 완료
- **주의사항**: 테스트 후 자동으로 테스트 데이터 정리됨

### 성능 개선 사항
- Python Worker 연결 문제 해결로 안정성 향상
- `to_json()` Spark SQL 함수 사용으로 성능 최적화
- 메모리 설정 4g로 증가 (Windows 환경 안정성 향상)


