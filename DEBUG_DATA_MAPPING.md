# 데이터 매핑 흐름 및 트러블슈팅 가이드

## 1. 데이터 흐름 개요

```
Kafka 메시지 (JSON)
    ↓
[1단계] JSON 파싱 (parse_json_stream)
    ↓
UserEvent 스키마로 변환된 DataFrame
    ↓
[2단계] 타임스탬프 변환 (convert_timestamp)
    ↓
timestamp_ts 컬럼 추가 (TimestampType)
    ↓
[3단계] 세션 집계 (track_sessions)
    ↓
윈도우 기반 그룹화 및 집계
    ↓
[4단계] MySQL 저장 (save_sessions_to_mysql)
    ↓
user_sessions 테이블에 INSERT
```

## 2. Kafka 메시지 → UserEvent 스키마 매핑

### 입력 (Kafka 메시지):
```json
{
    "event_id": "0cad8c5c-3043-47f4-846a-4451a31e2d4d",
    "timestamp": "2026-01-06T04:55:51.579395Z",
    "user_id": "test-spark-user-001",
    "session_id": "test-spark-session-001",
    "event_type": "watch",
    "content_id": "spark-content-004",
    "genre": "action",
    "duration_minutes": 120,
    "watched_minutes": 5,
    "metadata": {
        "user_segment": "VIP",
        "ab_test_group": "A",
        "content_type": "movie",
        "device": "mobile"
    }
}
```

### 변환 후 (DataFrame):
```
event_id: StringType
timestamp: StringType (ISO 8601)
user_id: StringType
session_id: StringType
event_type: StringType
content_id: StringType
genre: StringType
duration_minutes: IntegerType
watched_minutes: IntegerType
metadata: MapType(StringType, StringType)
```

## 3. 타임스탬프 변환

### 입력:
- `timestamp`: "2026-01-06T04:55:51.579395Z" (StringType)

### 변환 후:
- `timestamp_ts`: TimestampType (Spark Timestamp)
- 형식: `to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")`

## 4. 세션 집계 (track_sessions)

### 입력 DataFrame:
- `session_id`, `user_id`, `timestamp_ts`, `event_type`, `content_id`, `watched_minutes` 등

### 윈도우 그룹화:
```python
.groupBy(
    col("session_id"),
    col("user_id"),
    window(col("timestamp_ts"), "30 seconds").alias("time_window")
)
```

### 집계 결과:
```python
.agg(
    count("*").alias("event_count"),                    # 세션 내 이벤트 수
    sum("watched_minutes").alias("total_watched_minutes"),  # 총 시청 시간
    collect_set(when(event_type=="click", content_id)).alias("browsed_contents"),
    collect_set(when(event_type=="watch", content_id)).alias("watched_contents"),
    collect_set(when(event_type=="watch_complete", content_id)).alias("completed_contents"),
    min("timestamp_ts").alias("start_time"),            # 세션 시작 시간
    max("timestamp_ts").alias("end_time")               # 세션 종료 시간
)
```

### 출력 DataFrame 컬럼:
- `session_id`: StringType
- `user_id`: StringType
- `start_time`: TimestampType
- `end_time`: TimestampType
- `event_count`: LongType
- `total_watched_minutes`: LongType
- `browsed_contents`: ArrayType(StringType)
- `watched_contents`: ArrayType(StringType)
- `completed_contents`: ArrayType(StringType)

## 5. MySQL 저장 (save_sessions_to_mysql)

### 입력 (batch_df):
- 윈도우 집계 결과 DataFrame

### 변환:
1. **배열 → JSON 문자열**: `browsed_contents`, `watched_contents`, `completed_contents`
   - UDF 사용: `array_to_json_string()`
   - 예: `["content-001", "content-002"]` → `'["content-001","content-002"]'`

2. **컬럼 선택**:
```python
sessions_df = batch_df.select(
    col("session_id"),              # → session_id (VARCHAR(100))
    col("user_id"),                 # → user_id (VARCHAR(50))
    col("start_time"),              # → start_time (DATETIME)
    col("end_time"),                # → end_time (DATETIME)
    col("event_count"),             # → event_count (INT)
    col("total_watched_minutes"),   # → total_watched_minutes (INT)
    array_to_json_udf(col("browsed_contents")).alias("browsed_contents"),    # → browsed_contents (LONGTEXT JSON)
    array_to_json_udf(col("watched_contents")).alias("watched_contents"),    # → watched_contents (LONGTEXT JSON)
    array_to_json_udf(col("completed_contents")).alias("completed_contents") # → completed_contents (LONGTEXT JSON)
)
```

### MySQL 테이블 매핑:

| DataFrame 컬럼 | MySQL 컬럼 | 타입 | 설명 |
|---------------|-----------|------|------|
| `session_id` | `session_id` | VARCHAR(100) | 세션 ID (PRIMARY KEY) |
| `user_id` | `user_id` | VARCHAR(50) | 사용자 ID |
| `start_time` | `start_time` | DATETIME | 세션 시작 시간 |
| `end_time` | `end_time` | DATETIME | 세션 종료 시간 |
| `event_count` | `event_count` | INT | 세션 내 이벤트 수 |
| `total_watched_minutes` | `total_watched_minutes` | INT | 총 시청 시간 (분) |
| `browsed_contents` (JSON 문자열) | `browsed_contents` | LONGTEXT | 클릭한 콘텐츠 목록 (JSON 배열) |
| `watched_contents` (JSON 문자열) | `watched_contents` | LONGTEXT | 실제 시청한 콘텐츠 목록 (JSON 배열) |
| `completed_contents` (JSON 문자열) | `completed_contents` | LONGTEXT | 완료한 콘텐츠 목록 (JSON 배열) |

**자동 생성 컬럼:**
- `id`: BIGINT AUTO_INCREMENT (PRIMARY KEY)
- `created_at`: DATETIME DEFAULT CURRENT_TIMESTAMP
- `updated_at`: DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP

## 6. 현재 문제점

### 문제 1: 컬럼명 불일치
- ❌ `migrations.py`: `total_watch_minutes` (잘못됨)
- ✅ 실제 테이블: `total_watched_minutes` (올바름)
- ✅ `streaming.py`: `total_watched_minutes` (올바름)
- ❌ `test_spark_streaming_integration.py`: `total_watch_minutes` (일부 잘못됨)

**해결**: migrations.py와 테스트 파일 수정 완료

### 문제 2: `save_sessions_to_mysql` 함수가 호출되지 않음
- 로그에 "배치 X: save_sessions_to_mysql 호출됨" 메시지가 없음
- `foreachBatch`가 호출되지 않음

**가능한 원인:**
1. 윈도우 집계 결과가 비어있음 (빈 DataFrame)
2. `outputMode("append")`로 변경했지만 여전히 데이터가 없음
3. 워터마크 설정 문제로 윈도우가 완료되지 않음

**확인 방법:**
- `track_sessions` 함수에서 집계 결과를 로깅
- `process_stream` 함수에서 세션 DataFrame 스키마 확인
- 실제로 이벤트가 읽히는지 확인

## 7. 디버깅 체크리스트

- [ ] Kafka 메시지가 실제로 읽히는가?
- [ ] JSON 파싱이 성공하는가?
- [ ] 타임스탬프 변환이 성공하는가?
- [ ] 윈도우 집계 결과가 비어있지 않은가?
- [ ] `foreachBatch`가 호출되는가?
- [ ] 배치 DataFrame에 데이터가 있는가?
- [ ] MySQL 저장 시 에러가 발생하는가?

## 8. 다음 단계

1. 로그 레벨을 DEBUG로 설정하여 상세 로그 확인
2. 각 단계에서 DataFrame 샘플 데이터 출력
3. 윈도우 집계 결과 확인
4. `foreachBatch` 호출 여부 확인


