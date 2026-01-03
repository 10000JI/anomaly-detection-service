# Task 002: MySQL 데이터베이스 스키마 생성

> **작업 번호:** 002  
> **작업명:** MySQL Database Schema Creation  
> **의존성:** Task 001 (Configuration Files Setup)  
> **상태:** ✅ 완료  
> **생성일:** 2026-01-01  
> **완료일:** 2026-01-03 (COMMENT 통합)

---

## 1. Task Overview

### Task Title
**Title:** MySQL 데이터베이스 스키마 생성 및 마이그레이션 구현

### Goal Statement
**Goal:** 
실시간 사용자 행동 분석 & 개인화 추천 시스템에 필요한 MySQL 데이터베이스 테이블을 생성하고, 스키마 마이그레이션 기능을 구현하여 안정적인 데이터 저장소를 구축합니다.

**비즈니스 가치:**
- 사용자 프로필, 콘텐츠 정보, 이벤트 히스토리를 체계적으로 저장
- 추천 결과 및 A/B 테스트 성과를 추적하여 알고리즘 개선
- 데이터 무결성 보장 및 효율적인 쿼리 성능 제공

---

## 2. Current State Analysis

### 기존 상태
- ✅ MySQL 설정 파일 (`config/mysql_config.py`) 구현 완료
- ✅ 환경 변수 로딩 설정 완료
- ✅ MySQL 클라이언트 구현 완료
- ✅ 데이터베이스 테이블 생성 완료
- ✅ 스키마 마이그레이션 로직 구현 완료

### MySQL 연결 정보
- **Host:** 192.168.150.110 (외부 서버)
- **Port:** 3306
- **Database:** cursor_practice
- **User:** didim
- **Password:** ******** (`.env`에서 관리)
- **상태:** ✅ 연결 성공 및 7개 테이블 생성 완료

---

## 3. Technical Requirements

### Functional Requirements
- 시스템은 MySQL 데이터베이스에 자동으로 연결한다
- 시스템은 데이터베이스 스키마를 자동으로 생성할 수 있다
- 시스템은 테이블 존재 여부를 확인하고 누락된 테이블만 생성한다
- 시스템은 스키마 생성 결과를 로깅한다
- 개발자는 테스트 코드를 통해 스키마 생성을 검증할 수 있다

### Non-Functional Requirements
- **Performance:** 스키마 생성 시간 < 5초
- **Reliability:** 연결 실패 시 재시도 로직 (최대 3회)
- **Maintainability:** 스키마 변경 시 쉽게 수정 가능한 구조
- **Idempotency:** 동일한 스키마를 여러 번 실행해도 안전

---

## 4. Database Schema Design

### 4.1. user_profiles 테이블
**목적:** 사용자 기본 프로필 및 세그먼트 정보 저장

```sql
CREATE TABLE IF NOT EXISTS user_profiles (
    user_id VARCHAR(50) PRIMARY KEY,
    user_segment VARCHAR(20),
    signup_date DATE,
    total_purchases INT DEFAULT 0,
    total_spent DECIMAL(12,2) DEFAULT 0,
    favorite_categories JSON,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_segment (user_segment)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

**컬럼 설명:**
- `user_id`: 사용자 고유 ID (Primary Key)
- `user_segment`: 사용자 세그먼트 (VIP, Regular, New 등)
- `signup_date`: 가입 날짜
- `total_purchases`: 총 구매 횟수
- `total_spent`: 총 구매 금액
- `favorite_categories`: 선호 카테고리 (JSON 배열)
- `created_at`: 레코드 생성 시간
- `updated_at`: 레코드 수정 시간

**인덱스:**
- `idx_segment`: 세그먼트별 사용자 조회 최적화

---

### 4.2. contents 테이블
**목적:** 콘텐츠 메타데이터 저장 (영화, 드라마, 다큐멘터리)

```sql
CREATE TABLE IF NOT EXISTS contents (
    content_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content_type ENUM('movie', 'series', 'documentary') NOT NULL,
    genre VARCHAR(100),
    sub_genre VARCHAR(100),
    duration_minutes INT,
    release_year INT,
    rating FLOAT,
    review_count INT DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_genre (genre),
    INDEX idx_type (content_type),
    INDEX idx_rating (rating)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

**컬럼 설명:**
- `content_id`: 콘텐츠 고유 ID (Primary Key)
- `title`: 콘텐츠 제목
- `content_type`: 콘텐츠 유형 (movie, series, documentary)
- `genre`: 주 장르
- `sub_genre`: 서브 장르
- `duration_minutes`: 재생 시간 (분)
- `release_year`: 출시 연도
- `rating`: 평균 평점
- `review_count`: 리뷰 개수

**인덱스:**
- `idx_genre`: 장르별 콘텐츠 검색 최적화
- `idx_type`: 콘텐츠 타입별 필터링 최적화
- `idx_rating`: 평점 정렬 최적화

---

### 4.3. user_events 테이블
**목적:** 사용자 행동 이벤트 히스토리 저장

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

**컬럼 설명:**
- `id`: 자동 증가 Primary Key
- `user_id`: 사용자 ID
- `session_id`: 세션 ID
- `event_type`: 이벤트 타입 (click, watch, watchlist, watch_complete, rating)
- `content_id`: 콘텐츠 ID
- `watched_minutes`: 시청 시간 (분)
- `timestamp`: 이벤트 발생 시간
- `metadata`: 추가 메타데이터 (JSON)

**인덱스:**
- `idx_user_timestamp`: 사용자별 시간순 이벤트 조회 최적화
- `idx_content`: 콘텐츠별 이벤트 조회
- `idx_session`: 세션별 이벤트 추적

---

### 4.4. user_sessions 테이블
**목적:** 사용자 세션 정보 및 분석 결과 저장

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

**컬럼 설명:**
- `session_id`: 세션 고유 ID (Primary Key)
- `user_id`: 사용자 ID
- `start_time`: 세션 시작 시간
- `end_time`: 세션 종료 시간
- `event_count`: 세션 내 이벤트 수
- `browsed_contents`: 탐색한 콘텐츠 목록 (JSON 배열)
- `watched_contents`: 시청한 콘텐츠 목록 (JSON 배열)
- `completed_contents`: 완료한 콘텐츠 목록 (JSON 배열)
- `total_watch_minutes`: 총 시청 시간 (분)

**인덱스:**
- `idx_user_start`: 사용자별 세션 조회 최적화

---

### 4.5. recommendations 테이블
**목적:** 사용자별 추천 결과 저장 및 추적

```sql
CREATE TABLE IF NOT EXISTS recommendations (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    content_id VARCHAR(50) NOT NULL,
    recommendation_score FLOAT,
    algorithm VARCHAR(50),
    ab_test_group VARCHAR(20),
    is_clicked BOOLEAN DEFAULT FALSE,
    is_watched BOOLEAN DEFAULT FALSE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_created (user_id, created_at),
    INDEX idx_algorithm (algorithm)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

**컬럼 설명:**
- `id`: 자동 증가 Primary Key
- `user_id`: 사용자 ID
- `content_id`: 추천된 콘텐츠 ID
- `recommendation_score`: 추천 점수
- `algorithm`: 사용된 알고리즘 (collaborative_filtering, session_based 등)
- `ab_test_group`: A/B 테스트 그룹 (algorithm_v1, algorithm_v2 등)
- `is_clicked`: 클릭 여부
- `is_watched`: 시청 여부

**인덱스:**
- `idx_user_created`: 사용자별 최근 추천 조회
- `idx_algorithm`: 알고리즘별 성능 분석

---

### 4.6. ab_test_groups 테이블
**목적:** A/B 테스트 그룹 정의 및 사용자 할당

```sql
CREATE TABLE IF NOT EXISTS ab_test_groups (
    id INT AUTO_INCREMENT PRIMARY KEY,
    group_name VARCHAR(50) NOT NULL UNIQUE,
    description TEXT,
    algorithm VARCHAR(50),
    config JSON,
    is_active BOOLEAN DEFAULT TRUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

**컬럼 설명:**
- `id`: 자동 증가 Primary Key
- `group_name`: 그룹명 (algorithm_v1, algorithm_v2 등)
- `description`: 그룹 설명
- `algorithm`: 사용 알고리즘
- `config`: 알고리즘 설정 (JSON)
- `is_active`: 활성화 여부

**인덱스:**
- `idx_active`: 활성 그룹 조회 최적화

---

### 4.7. ab_test_metrics 테이블
**목적:** A/B 테스트 성능 지표 저장

```sql
CREATE TABLE IF NOT EXISTS ab_test_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    test_group VARCHAR(20) NOT NULL,
    metric_name VARCHAR(100),
    metric_value FLOAT,
    sample_size INT,
    timestamp DATETIME NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_group_timestamp (test_group, timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

**컬럼 설명:**
- `id`: 자동 증가 Primary Key
- `test_group`: 테스트 그룹명
- `metric_name`: 지표명 (click_rate, watch_rate, precision 등)
- `metric_value`: 지표 값
- `sample_size`: 샘플 크기
- `timestamp`: 측정 시간

**인덱스:**
- `idx_group_timestamp`: 그룹별 시계열 분석 최적화

---

## 5. Implementation Plan

### Phase 1: MySQL 클라이언트 구현 ✅
**Tasks:**
- [x] `src/storage/` 디렉토리 생성
- [x] `src/storage/mysql_client.py` 구현
  - [x] `MySQLClient` 클래스 생성
  - [x] Connection Pool 설정
  - [x] 연결 재시도 로직
  - [x] 에러 핸들링

**파일 구조:**
```
src/
└── storage/
    ├── __init__.py
    └── mysql_client.py
```

### Phase 2: 스키마 마이그레이션 구현 ✅
**Tasks:**
- [x] `src/storage/migrations.py` 구현
  - [x] `DatabaseMigration` 클래스 생성
  - [x] 테이블 존재 여부 확인 함수
  - [x] 각 테이블 생성 함수 (7개)
  - [x] 전체 스키마 생성 함수
  - [x] 스키마 검증 함수

**파일:**
- `src/storage/migrations.py`

### Phase 3: 테스트 코드 작성 ✅
**Tasks:**
- [x] `tests/test_db_schema.py` 구현
  - [x] MySQL 연결 테스트
  - [x] 각 테이블 생성 테스트
  - [x] 테이블 구조 검증
  - [x] 중복 생성 안전성 테스트

**파일:**
- `tests/test_db_schema.py`

### Phase 4: 실행 및 검증 ✅
**Tasks:**
- [x] 스키마 생성 스크립트 실행
- [x] 테이블 생성 확인 (7/7 성공)
- [x] 인덱스 확인
- [x] 테스트 실행 (11/11 통과)
- [x] 문서화

---

## 6. Implementation Details

### 6.1. MySQLClient 클래스 설계

```python
class MySQLClient:
    """MySQL 데이터베이스 클라이언트"""
    
    def __init__(self):
        """설정 로드 및 초기화"""
        
    def get_connection(self):
        """Connection Pool에서 연결 가져오기"""
        
    def execute_query(self, query: str, params: tuple = None):
        """쿼리 실행 (INSERT, UPDATE, DELETE)"""
        
    def fetch_one(self, query: str, params: tuple = None):
        """단일 레코드 조회"""
        
    def fetch_all(self, query: str, params: tuple = None):
        """다중 레코드 조회"""
        
    def table_exists(self, table_name: str) -> bool:
        """테이블 존재 여부 확인"""
        
    def close(self):
        """연결 종료"""
```

### 6.2. DatabaseMigration 클래스 설계

```python
class DatabaseMigration:
    """데이터베이스 스키마 마이그레이션"""
    
    def __init__(self, mysql_client: MySQLClient):
        """MySQL 클라이언트 주입"""
        
    def create_user_profiles_table(self):
        """user_profiles 테이블 생성"""
        
    def create_contents_table(self):
        """contents 테이블 생성"""
        
    def create_user_events_table(self):
        """user_events 테이블 생성"""
        
    def create_user_sessions_table(self):
        """user_sessions 테이블 생성"""
        
    def create_recommendations_table(self):
        """recommendations 테이블 생성"""
        
    def create_ab_test_groups_table(self):
        """ab_test_groups 테이블 생성"""
        
    def create_ab_test_metrics_table(self):
        """ab_test_metrics 테이블 생성"""
        
    def run_migrations(self):
        """전체 마이그레이션 실행"""
        
    def verify_schema(self) -> dict:
        """스키마 검증 및 결과 반환"""
```

---

## 7. Testing Strategy

### 단위 테스트
```python
def test_mysql_connection():
    """MySQL 연결 테스트"""
    
def test_create_user_profiles_table():
    """user_profiles 테이블 생성 테스트"""
    
def test_create_contents_table():
    """contents 테이블 생성 테스트"""
    
def test_table_already_exists():
    """이미 존재하는 테이블 재생성 안전성 테스트"""
    
def test_full_migration():
    """전체 마이그레이션 테스트"""
    
def test_schema_verification():
    """스키마 검증 테스트"""
```

### 통합 테스트
- MySQL 서버 연결 확인
- 실제 데이터베이스에 테이블 생성
- 샘플 데이터 삽입/조회
- 인덱스 성능 확인

---

## 8. Error Handling

### 예상 에러 시나리오
1. **MySQL 서버 연결 실패**
   - 재시도 로직 (최대 3회, 지수 백오프)
   - 상세 에러 로깅
   
2. **테이블 생성 실패**
   - SQL 오류 로깅
   - 생성된 테이블까지만 유지 (부분 성공)
   
3. **권한 부족**
   - CREATE TABLE 권한 확인
   - 명확한 에러 메시지 제공

4. **네트워크 타임아웃**
   - Connection Pool 설정 최적화
   - 타임아웃 설정 (30초)

---

## 9. Success Criteria

### 기능 완성도
- ✅ 7개 테이블 모두 정상 생성
- ✅ 모든 인덱스 생성 확인
- ✅ 테이블 구조가 스키마 명세와 일치
- ✅ 중복 실행 시 에러 없음 (Idempotency)

### 코드 품질
- ✅ PEP 8 스타일 준수
- ✅ 타입 힌트 사용
- ✅ Docstring 작성 (Google 스타일)
- ✅ 에러 핸들링 포함

### 테스트 커버리지
- ✅ 모든 테이블 생성 테스트 통과
- ✅ 연결 실패 시나리오 테스트
- ✅ 스키마 검증 테스트 통과

### 성능
- ✅ 전체 스키마 생성 시간 < 5초
- ✅ Connection Pool 정상 작동

---

## 10. Files to Create/Modify

### 신규 생성 파일
```
src/storage/__init__.py              # 스토리지 패키지 초기화
src/storage/mysql_client.py          # MySQL 클라이언트
src/storage/migrations.py            # 스키마 마이그레이션
tests/test_db_schema.py              # 테스트 코드
scripts/run_migrations.py            # 마이그레이션 실행 스크립트
```

### 수정 파일
```
requirements.txt                     # mysql-connector-python 추가
```

---

## 11. AI Agent Instructions

### 구현 순서
1. `src/storage/` 디렉토리 생성
2. `mysql_client.py` 구현 (Connection Pool 포함)
3. `migrations.py` 구현 (7개 테이블 생성)
4. `test_db_schema.py` 테스트 코드 작성
5. `run_migrations.py` 실행 스크립트 작성
6. 테스트 실행 및 검증

### 주의사항
- 모든 SQL 쿼리는 `IF NOT EXISTS` 사용 (Idempotency 보장)
- Connection Pool 사용 (동시성 고려)
- 에러 발생 시 상세 로깅
- 테이블명, 컬럼명은 스키마 명세와 정확히 일치
- 인덱스는 반드시 포함

### 코드 스타일
- Python 3.9+ 타입 힌트 사용
- Google 스타일 Docstring
- try-except 블록으로 에러 핸들링
- logging 모듈로 상세 로깅

---

## 12. Progress Tracking

### Phase 1: MySQL 클라이언트 구현 ✅
- [x] 디렉토리 생성
- [x] MySQLClient 클래스 구현
- [x] Connection Pool 설정
- [x] 연결 테스트

### Phase 2: 스키마 마이그레이션 구현 ✅
- [x] DatabaseMigration 클래스 구현
- [x] user_profiles 테이블 생성 함수
- [x] contents 테이블 생성 함수
- [x] user_events 테이블 생성 함수
- [x] user_sessions 테이블 생성 함수
- [x] recommendations 테이블 생성 함수
- [x] ab_test_groups 테이블 생성 함수
- [x] ab_test_metrics 테이블 생성 함수
- [x] 전체 마이그레이션 함수

### Phase 3: 테스트 코드 ✅
- [x] 연결 테스트
- [x] 각 테이블 생성 테스트
- [x] 스키마 검증 테스트
- [x] 중복 실행 안전성 테스트

### Phase 4: 실행 및 검증 ✅
- [x] 스크립트 실행 (7/7 테이블 생성 성공)
- [x] 데이터베이스 확인 (11/11 테스트 통과)
- [x] 문서 업데이트

---

## 13. Next Steps

이 작업 완료 후:
1. **Task 003:** 샘플 데이터 생성 (users, contents)
2. **Task 004:** Kafka Producer 구현 (이벤트 시뮬레이터)
3. **Task 005:** Kafka Consumer 및 PySpark 스트리밍 구현

---

## 14. Execution Summary

### 실행 결과
**실행 일시:** 2026-01-01 20:51:57 ~ 20:52:16

**마이그레이션 결과:**
- ✅ user_profiles 테이블 생성 (8개 컬럼)
- ✅ contents 테이블 생성 (10개 컬럼)
- ✅ user_events 테이블 생성 (9개 컬럼)
- ✅ user_sessions 테이블 생성 (11개 컬럼)
- ✅ recommendations 테이블 생성 (9개 컬럼)
- ✅ ab_test_groups 테이블 생성 (7개 컬럼)
- ✅ ab_test_metrics 테이블 생성 (7개 컬럼)

**테스트 결과:**
- ✅ MySQL 연결 테스트 통과
- ✅ 7개 테이블 생성 테스트 통과
- ✅ 중복 생성 안전성 테스트 통과
- ✅ 전체 마이그레이션 테스트 통과
- ✅ 스키마 검증 테스트 통과
- 📊 **최종 결과: 11/11 테스트 통과**

**생성된 파일:**
```
src/storage/__init__.py              ✅ 생성 (15줄)
src/storage/mysql_client.py          ✅ 생성 (302줄)
src/storage/migrations.py            ✅ 생성 (463줄) - COMMENT 통합
tests/test_db_schema.py              ✅ 생성 (398줄)
scripts/run_migrations.py            ✅ 생성 (142줄)
migration_20260101_205157.log        ✅ 생성 (로그 파일)
```

**성능 지표:**
- 전체 스키마 생성 시간: 약 2.2초 (목표: 5초 이하) ✅
- Connection Pool 초기화 시간: 약 327ms ✅
- 테이블당 평균 생성 시간: 약 200ms ✅

### 검증 완료 항목
- ✅ 모든 테이블이 정확한 스키마로 생성됨
- ✅ 모든 인덱스가 올바르게 설정됨
- ✅ 61개 컬럼 COMMENT + 7개 테이블 COMMENT 추가
- ✅ Idempotency 보장 (중복 실행 안전)
- ✅ Connection Pool 정상 작동
- ✅ 에러 핸들링 및 재시도 로직 작동
- ✅ MySQL 서버 연결 (192.168.150.110:3306)
- ✅ 데이터베이스 접근 (cursor_practice)

### 추가 개선 사항 (2026-01-03)
- ✅ 테이블 생성 SQL에 모든 COMMENT 통합
- ✅ 61개 컬럼 COMMENT 자동 추가
- ✅ 7개 테이블 COMMENT 자동 추가
- ✅ `scripts/add_table_comments.py` 삭제 (통합으로 불필요)
- ✅ `migrations.py` 업데이트 (463줄)

---

## 15. 최종 파일 목록

### 생성된 파일
```
src/storage/
├── __init__.py              ✅ 생성 (15줄)
├── mysql_client.py          ✅ 생성 (302줄)
└── migrations.py            ✅ 생성 (463줄) - COMMENT 통합

scripts/
└── run_migrations.py        ✅ 생성 (142줄)

tests/
└── test_db_schema.py        ✅ 생성 (398줄)

ai_docs/tasks/
└── 002_mysql_database_schema_creation.md ✅ 작업 문서

Logs/
└── migration_20260101_205157.log ✅ 실행 로그
```

### 삭제된 파일
```
scripts/
└── add_table_comments.py    ❌ 삭제 (migrations.py에 통합)
```

---

**작업 시작일:** 2026-01-01  
**작업 완료일:** 2026-01-03  
**담당자:** AI Agent  
**상태:** ✅ 완료

**다음 단계:** Task 003 (샘플 데이터 생성) → ✅ 완료  
**다음 단계:** Task 004 (Kafka Producer 구현)


