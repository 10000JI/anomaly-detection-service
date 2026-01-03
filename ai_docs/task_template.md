# AI Task Planning Template - Real-Time User Behavior Analysis & Personalized Recommendation System

> **프로젝트:** 실시간 사용자 행동 분석 & 개인화 추천 시스템  
> **환경:** Windows Local (Cursor IDE)  
> **외부 인프라:** Kafka, MySQL, Redis, Prometheus (별도 서버)

---

## 1. Task Overview

### Task Title
**Title:** [예: "실시간 사용자 세션 추적 및 협업 필터링 구현"]

### Goal Statement
**Goal:** 
[이 작업을 통해 달성하고자 하는 최종 결과와 비즈니스/사용자 가치를 명확히 기술]

예시: "사용자의 시청 이벤트를 실시간으로 분석하여 개인화된 콘텐츠 추천을 제공하고, Netflix 수준의 실시간 추천 시스템을 구축한다."

---

## 2. Project Analysis & Current State

### Technology & Architecture
- **Language:** Python 3.9.6
- **Stream Processing:** Apache Spark 3.4.1 (PySpark - Local)
- **Message Broker:** Apache Kafka 3.8.1 (External Cluster)
  - Bootstrap Servers: `192.168.150.115:9092,192.168.150.120:9092,192.168.150.125:9092`
  - Message Format: Pure JSON (No Schema Registry)
- **Database:** MySQL (External Server)
  - Host: `your_mysql_host:3306`
  - User: `your_mysql_user`
  - Password: `your_secure_password` (`.env`에서 관리)
  - Database: `your_database_name`
- **Cache:** Redis (External Server)
  - Host: `192.168.150.110:6379`
  - Purpose: 실시간 추천 결과 캐싱
- **Monitoring:** Prometheus (External Server)
  - Endpoint: `192.168.150.110:19090`
- **ML Algorithms:** Collaborative Filtering, Session Analysis (scikit-learn, pandas)
- **API:** REST API (Flask/FastAPI)
- **UI:** Vanilla JavaScript (No frameworks)
- **Development Environment:** Windows Local (Docker 미사용)

### Current State
[현재 코드베이스 상태, 기존 기능, 변경이 필요한 부분 분석]

TODO: 프로젝트 초기 상태 또는 현재 구현된 기능 상태를 기술

---

## 3. Project Objectives & System Design

### System Purpose
**이 시스템이 만들어지는 이유:**

실시간 사용자 행동 분석 & 개인화 추천 시스템은 Netflix, YouTube와 같은 글로벌 플랫폼이 사용하는 실시간 콘텐츠 추천 기술을 학습하고 구현하여, 사용자 경험을 극대화하고 시청률을 향상시킵니다.

### Core Objectives
**시스템의 핵심 목표:**

1. **실시간 행동 추적**: 사용자의 클릭, 시청, 평가 이벤트를 실시간으로 수집하고 세션화
2. **지능형 추천**: 협업 필터링과 세션 패턴 분석을 통한 개인화 콘텐츠 추천
3. **고속 응답**: Redis 캐싱을 활용하여 추천 결과를 100ms 이내 응답
4. **A/B 테스트**: 실시간으로 추천 알고리즘 성능을 비교하고 최적화

### Target Event Types
**수집 대상 이벤트:**

**클릭 이벤트**: 콘텐츠 클릭, 장르 탐색, 검색 쿼리
**시청 이벤트**: 콘텐츠 재생, 시청 시간, 중단 지점
**관심 목록 이벤트**: 찜하기 추가/삭제, 나중에 보기 등록
**시청 완료 이벤트**: 시청 완료, 시청 비율, 재시청 여부
**평가 이벤트**: 콘텐츠 평점, 리뷰 작성, 좋아요/싫어요

이벤트 형식 예시
```json
{
  "event_id": "evt-123456",
  "timestamp": "2025-01-15T10:30:45.123Z",
  "user_id": "user-98765",
  "session_id": "sess-abc-def",
  "event_type": "content_watch",
  "content_id": "movie-555",
  "genre": "action",
  "duration_minutes": 120,
  "watched_minutes": 45,
  "metadata": {
    "referrer": "homepage",
    "device": "smart_tv",
    "ab_test_group": "algorithm_v2",
    "quality": "4K"
  }
}
```

### Expected System Benefits
**시스템을 통해 얻을 수 있는 가치:**

- **시청률 향상**: 개인화 추천으로 콘텐츠 시청 완료율 20-30% 증가 기대
- **사용자 경험**: 실시간 추천으로 콘텐츠 탐색 시간 단축 및 만족도 향상
- **데이터 기반 의사결정**: A/B 테스트를 통한 알고리즘 성능 비교 및 최적화
- **실시간 인사이트**: 사용자 시청 패턴을 실시간으로 파악하여 콘텐츠 전략 수립

### Success Criteria
**시스템 성공 기준:**

#### 성능 기준:
- Kafka 클러스터로부터 클릭 이벤트를 초당 5,000건 이상 안정적으로 수집
- PySpark 스트리밍 처리 지연시간 평균 500ms 이하 유지
- 추천 API 응답 시간 100ms 이하 (Redis 캐시 활용)
- Prometheus 메트릭 업데이트 주기 5초 이내

#### 안정성 기준:
- 네트워크 장애 시 30초 이내 자동 재연결 성공률 95% 이상
- Kafka Consumer 연결 끊김 발생 시 이벤트 손실 0건 (offset 관리)
- Redis 캐시 실패 시 MySQL Fallback 자동 전환
- PySpark 작업 실패 시 자동 재시작 및 에러 로깅

#### 추천 품질 기준:
- 추천 정확도 (Precision@10) 25% 이상
- 추천 다양성 (Diversity Score) 0.7 이상
- 클릭률(CTR) 기준 추천 성능 5% 이상 향상 (A/B 테스트)
- 사용자당 세션 길이 20% 증가

#### 기능 완성도:
- REST API 모든 엔드포인트 응답 시간 100ms 이하
- 웹 UI 실시간 업데이트 지연시간 3초 이내
- Prometheus 대시보드에 최소 15개 이상의 핵심 메트릭 노출
- 시스템 24시간 연속 가동 시 메모리 누수 없음 (메모리 증가율 5% 이하)

#### 사용자 경험 기준:
- 사용자 클릭 후 추천 결과 업데이트 3초 이내
- 개인화 추천 목록 10개 이상 제공
- A/B 테스트 그룹별 성능 비교 실시간 확인 가능
- 모든 추천 결과는 설명 가능성(Explainability) 제공

---

## 4. Development Mode Context

### Development Mode Context
- **🚨 Project Stage:** 신규 개발 (MVP 구축 단계)
- **Breaking Changes:** 허용 (초기 개발 단계)
- **Data Handling:** 테스트 데이터 사용 (프로덕션 데이터 미사용)
- **User Base:** 개발자 본인 (로컬 테스트 환경)
- **Priority:** 기능 구현 속도 > 완벽한 안정성 (빠른 프로토타이핑 우선)

---

## 5. Technical Requirements

### Functional Requirements
**사용자/시스템 기능:**
- 시스템은 외부 Kafka 클러스터(`192.168.150.115:9092` 등)에 자동으로 연결한다
- 시스템은 JSON 형식의 시청 이벤트를 실시간으로 소비한다
- 사용자는 REST API를 통해 이벤트 수집을 시작/중지할 수 있다
- 시스템은 3분 윈도우로 사용자 세션을 자동 추적한다
- 시스템은 협업 필터링 알고리즘으로 유사 사용자를 찾고 추천을 생성한다
- 추천 결과는 Redis에 캐싱되고 MySQL에 영구 저장된다
- 사용자는 REST API를 통해 개인화 추천 목록을 조회할 수 있다
- 시스템은 A/B 테스트 그룹별 성능 지표를 자동 수집한다
- Prometheus 메트릭은 `/metrics` 엔드포인트를 통해 노출된다
- 사용자는 웹 UI에서 실시간 시청 히트맵과 추천 결과를 시각화할 수 있다

TODO: 추가 기능 요구사항 정의

### Non-Functional Requirements
- **Performance:** 
  - 이벤트 처리 지연시간 < 500ms
  - PySpark 스트리밍 배치 간격: 3초
  - 추천 API 응답 시간 < 100ms (Redis 캐시)
- **Security:** 
  - 모든 설정값은 `.env` 파일에서 관리 (Java의 application.properties와 유사)
  - 비밀번호, API 키 등 민감정보는 절대 코드에 하드코딩 금지
  - `.env` 파일은 `.gitignore`에 포함하여 버전 관리에서 제외
  - `.env.template` 파일로 설정 예시 제공
  - 사용자 개인정보는 익명화 처리 (GDPR 준수)
- **Usability:** 
  - 웹 UI는 직관적이고 반응형이어야 함
  - API 엔드포인트는 명확한 에러 메시지 제공
  - 추천 결과는 설명 가능성(Explainability) 제공
- **Responsive Design:** 
  - Desktop 우선 (1920x1080)
  - Mobile 대응은 2단계 개발
- **Theme Support:** 
  - Dark 모드 기본 제공

### Technical Constraints
- Docker 사용 불가 (모든 서비스는 로컬 Python 환경 또는 외부 서버)
- Kafka, MySQL, Redis, Prometheus는 외부 서버 사용 (IP 고정)
- PySpark는 로컬에서만 실행 가능 (Standalone 모드)
- Schema Registry 미사용 (순수 JSON 파싱)
- **설정 파일 구조:** `.env` + `python-dotenv` 라이브러리 사용 (Java의 application.properties 방식과 동일)

---

## 6. Data & Database Changes

### Database Schema Changes
```sql
-- MySQL 스키마 정의 (cursor_practice 데이터베이스)

-- 사용자 프로필 테이블
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
);

-- 콘텐츠 정보 테이블
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
);

-- 사용자 이벤트 히스토리 테이블 (요약)
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

-- 추천 결과 테이블
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
);

-- A/B 테스트 성능 테이블
CREATE TABLE IF NOT EXISTS ab_test_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    test_group VARCHAR(20) NOT NULL,
    metric_name VARCHAR(100),
    metric_value FLOAT,
    sample_size INT,
    timestamp DATETIME NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_group_timestamp (test_group, timestamp)
);

-- 시스템 메트릭 테이블
CREATE TABLE IF NOT EXISTS system_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    metric_name VARCHAR(100),
    metric_value FLOAT,
    timestamp DATETIME NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_metric_timestamp (metric_name, timestamp)
);

TODO: 추가 테이블 정의
```

### Data Model Updates
```python
# Python 데이터 모델 정의

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum

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

@dataclass
class UserProfile:
    """사용자 프로필"""
    user_id: str
    user_segment: str
    signup_date: datetime
    total_purchases: int
    total_spent: float
    favorite_categories: List[str]

@dataclass
class Content:
    """콘텐츠 정보"""
    content_id: str
    title: str
    content_type: ContentType
    genre: str
    sub_genre: str
    duration_minutes: int
    release_year: int
    rating: float
    review_count: int

@dataclass
class Recommendation:
    """개인화 추천 결과"""
    user_id: str
    content_id: str
    recommendation_score: float
    algorithm: str
    ab_test_group: str
    reason: Optional[str] = None  # 추천 이유 (설명 가능성)

@dataclass
class SessionAnalysis:
    """세션 분석 결과"""
    session_id: str
    user_id: str
    start_time: datetime
    end_time: datetime
    event_count: int
    browsed_contents: List[str]
    watched_contents: List[str]
    completed_contents: List[str]
    total_watch_minutes: int

@dataclass
class PrometheusMetric:
    """Prometheus 메트릭"""
    name: str
    value: float
    labels: Dict[str, str]
    timestamp: datetime

TODO: 추가 데이터 모델 정의
```

### Data Migration Plan
**초기 설정 (마이그레이션 불필요):**
1. MySQL 데이터베이스 `cursor_practice` 생성 확인
2. 위 스키마 실행하여 테이블 생성
3. 샘플 사용자 프로필 데이터 삽입 (100명)
4. 샘플 콘텐츠 데이터 삽입 (1,000개: 영화 600, 드라마 300, 다큐 100)
5. 초기 이벤트 데이터 생성 (시뮬레이션)
6. Redis 캐시 워밍업 (인기 콘텐츠 추천 미리 계산)

---

## 7. API & Backend Changes

### Data Access Pattern Rules
```
이 프로젝트의 코드 구조:
- src/storage/mysql_client.py: MySQL CRUD 작업
- src/storage/redis_client.py: Redis 캐시 작업
- src/kafka/consumer.py: Kafka 이벤트 소비
- src/kafka/producer.py: Kafka 이벤트 생성 (시뮬레이션)
- src/spark/streaming.py: PySpark 스트리밍 로직 (세션 추적, 집계)
- src/recommendation/: 추천 알고리즘
  - collaborative_filtering.py: 협업 필터링
  - session_based.py: 세션 기반 추천
  - ab_test.py: A/B 테스트 로직
- src/metrics/prometheus_exporter.py: Prometheus 메트릭 노출
- src/api/rest_api.py: REST API 엔드포인트
```

### Backend Operations
TODO: 필요한 백엔드 작업 목록 정의

**이벤트 관리:**
- `save_user_event(event: UserEvent)`: 사용자 이벤트를 MySQL에 저장
- `get_user_events(user_id: str, limit: int)`: 특정 사용자의 최근 이벤트 조회
- `get_session_events(session_id: str)`: 세션별 이벤트 조회

**추천 생성:**
- `generate_recommendations(user_id: str, algorithm: str)`: 사용자별 추천 생성
- `get_recommendations(user_id: str, limit: int)`: 추천 목록 조회 (Redis → MySQL)
- `cache_recommendations(user_id: str, recommendations: List[Recommendation])`: Redis에 추천 캐싱
- `track_recommendation_click(user_id: str, product_id: str)`: 추천 클릭 추적

**세션 분석:**
- `analyze_session(session_id: str)`: 세션 분석 및 요약
- `get_active_sessions()`: 현재 활성 세션 목록

**A/B 테스트:**
- `assign_ab_group(user_id: str)`: 사용자에게 A/B 테스트 그룹 할당
- `track_ab_metric(group: str, metric_name: str, value: float)`: A/B 테스트 지표 저장
- `compare_ab_groups()`: 그룹 간 성능 비교

**시스템 제어:**
- `start_kafka_consumer()`: Kafka 소비자 시작
- `stop_kafka_consumer()`: Kafka 소비자 중지
- `start_event_simulator()`: 이벤트 시뮬레이터 시작
- `update_prometheus_metric(metric: PrometheusMetric)`: 메트릭 업데이트

### External Service Integrations
**Kafka Integration:**
- Connection: `kafka-python` 라이브러리 사용
- Topic: `user-events-topic`
- Consumer Group: `recommendation-engine-group`
- Producer: 이벤트 시뮬레이터 (초당 100-1000건)

**MySQL Integration:**
- Connection Pooling: `mysql-connector-python`
- Auto-reconnect on failure
- Read/Write 분리 고려 (추후)

**Redis Integration:**
- Connection: `redis-py` 라이브러리
- TTL 설정: 추천 결과 캐시 10분
- Fallback: Redis 장애 시 MySQL 직접 조회

**Prometheus Integration:**
- `prometheus_client` 라이브러리
- HTTP endpoint: `localhost:8000/metrics`

---

## 8. Frontend Changes

### New Components
TODO: 생성할 UI 컴포넌트 목록

**메인 대시보드:**
- `dashboard.html`: 메인 대시보드 페이지
- `WatchHeatmap`: 실시간 시청 히트맵 (어떤 콘텐츠가 시청되는지)
- `SessionTimeline`: 사용자 세션 타임라인 (이벤트 흐름)
- `RecommendationPanel`: 실시간 추천 결과 표시
- `ABTestComparison`: A/B 테스트 그룹 성능 비교 차트
- `MetricsPanel`: Prometheus 메트릭 패널
- `ControlPanel`: 시작/중지/시뮬레이션 버튼

**사용자 분석:**
- `UserProfileCard`: 사용자 프로필 및 세그먼트 표시
- `WatchHistory`: 시청 이력 그래프
- `GenrePreference`: 선호 장르 차트

### Page Updates
- `index.html`: 메인 대시보드 페이지 (단일 페이지 애플리케이션)
- `user-detail.html`: 특정 사용자 상세 분석 페이지 (2단계)

### State Management
- Vanilla JavaScript로 상태 관리
- WebSocket 또는 Server-Sent Events를 통한 실시간 업데이트
- `fetch()` API를 사용한 REST API 호출

---

## 9. Implementation Plan

### Phase 1: 기본 인프라 설정
**Tasks:**
- [ ] 프로젝트 폴더 구조 생성
- [ ] `requirements.txt` 작성 및 패키지 설치
- [ ] `.env.template` 파일 생성 (설정 예시)
- [ ] `.env` 파일 생성 (실제 설정값 - gitignore에 추가)
- [ ] 환경 변수 로딩 설정 (`config/__init__.py`)
- [ ] 설정 클래스 구현 (`config/` 폴더)
- [ ] Kafka/MySQL/Redis/Prometheus 연결 테스트 스크립트 작성
- [ ] 샘플 데이터 생성 스크립트 (사용자, 상품)

**Files:**
- `.env.template` (버전 관리 포함)
- `.env` (버전 관리 제외 - 로컬 전용)
- `.gitignore` (`.env` 추가)
- `config/__init__.py` (dotenv 자동 로드)
- `config/kafka_config.py`
- `config/mysql_config.py`
- `config/redis_config.py`
- `config/spark_config.py`
- `config/prometheus_config.py`
- `requirements.txt` (python-dotenv, redis-py 포함)
- `scripts/generate_sample_data.py`

### Phase 2: 데이터 수집 및 이벤트 생성
**Tasks:**
- [ ] Kafka Producer 구현 - 이벤트 시뮬레이터 (`src/kafka/producer.py`)
- [ ] Kafka Consumer 구현 (`src/kafka/consumer.py`)
- [ ] PySpark 로컬 세션 설정 (`src/spark/streaming.py`)
- [ ] Kafka → PySpark 데이터 전달 로직
- [ ] MySQL 연결 및 CRUD 구현 (`src/storage/mysql_client.py`)
- [ ] Redis 연결 및 캐시 로직 구현 (`src/storage/redis_client.py`)

**Files:**
- `src/kafka/producer.py` (이벤트 시뮬레이터)
- `src/kafka/consumer.py`
- `src/spark/streaming.py`
- `src/storage/mysql_client.py`
- `src/storage/redis_client.py`

### Phase 3: 세션 추적 및 실시간 분석
**Tasks:**
- [ ] 3분 윈도우 기반 세션 추적 로직 (`src/spark/streaming.py`)
- [ ] 사용자별 이벤트 집계 (클릭수, 조회수, 구매수)
- [ ] 세션 분석 결과 MySQL 저장
- [ ] 실시간 통계 계산 (카테고리별, 시간대별)
- [ ] PySpark Window 함수 활용 (이동 평균, 집계)

**Files:**
- `src/spark/streaming.py` (세션 로직 추가)
- `src/analytics/session_analyzer.py`

### Phase 4: 추천 알고리즘 구현
**Tasks:**
- [ ] 협업 필터링 알고리즘 구현 (`src/recommendation/collaborative_filtering.py`)
- [ ] 세션 기반 추천 구현 (`src/recommendation/session_based.py`)
- [ ] A/B 테스트 그룹 할당 로직 (`src/recommendation/ab_test.py`)
- [ ] 추천 결과 Redis 캐싱
- [ ] 추천 클릭/구매 추적

**Files:**
- `src/recommendation/collaborative_filtering.py`
- `src/recommendation/session_based.py`
- `src/recommendation/ab_test.py`

### Phase 5: API 개발
**Tasks:**
- [ ] 추천 조회 API (`GET /api/recommendations/{user_id}`)
- [ ] 이벤트 전송 API (`POST /api/events`)
- [ ] 세션 분석 API (`GET /api/sessions/{session_id}`)
- [ ] A/B 테스트 비교 API (`GET /api/ab-test/compare`)
- [ ] 사용자 프로필 API (`GET /api/users/{user_id}`)
- [ ] Prometheus 메트릭 노출 (`GET /metrics`)
- [ ] API 테스트 (Postman/curl)

**Files:**
- `src/api/rest_api.py`
- `src/api/routes/recommendations.py`
- `src/api/routes/events.py`
- `src/api/routes/sessions.py`
- `src/api/routes/ab_test.py`
- `src/metrics/prometheus_exporter.py`

### Phase 6: UI 개발
**Tasks:**
- [ ] HTML 레이아웃 (`ui/index.html`)
- [ ] CSS 스타일링 (`ui/css/style.css`)
- [ ] 실시간 클릭 히트맵 구현 (`ui/js/heatmap.js`)
- [ ] 추천 결과 표시 (`ui/js/recommendations.js`)
- [ ] A/B 테스트 비교 차트 (`ui/js/ab-test-chart.js`)
- [ ] 세션 타임라인 (`ui/js/session-timeline.js`)
- [ ] REST API 통합
- [ ] 실시간 데이터 업데이트 (폴링 또는 WebSocket)

**Files:**
- `ui/index.html`
- `ui/css/style.css`
- `ui/js/dashboard.js`
- `ui/js/heatmap.js`
- `ui/js/recommendations.js`
- `ui/js/ab-test-chart.js`
- `ui/js/session-timeline.js`
- `ui/js/api-client.js`

### Phase 7: 통합 테스트 및 최적화
**Tasks:**
- [ ] End-to-end 테스트 (이벤트 생성 → 추천 → UI 표시)
- [ ] 추천 알고리즘 성능 테스트
- [ ] Redis 캐시 히트율 측정 및 최적화
- [ ] PySpark 윈도우 설정 튜닝
- [ ] A/B 테스트 통계적 유의성 검증
- [ ] 에러 핸들링 강화
- [ ] 문서화 (README, API 문서)

---

## 10. Task Completion Tracking

### Real-Time Progress Tracking
**AI Agent 지침:**
- 각 작업 완료 시 위 체크박스를 자동으로 업데이트하세요
- 파일 생성/수정 시 파일 경로를 명시하세요
- 에러 발생 시 즉시 보고하고 해결 방안을 제시하세요

**진행 상황:**
- 전체 진행률: [ ] 0% → [ ] 25% → [ ] 50% → [ ] 75% → [ ] 100%
- 현재 단계: Phase X
- 완료된 파일 수: 0 / Total

---

## 11. File Structure & Organization

```
recommendation-system/
├── .env                          # 🔒 실제 설정값 (gitignore, 로컬 전용)
├── .env.template                 # 📝 설정 예시 (버전 관리 포함)
├── .gitignore                    # Git 제외 목록 (.env 포함)
├── requirements.txt              # Python 패키지 (python-dotenv, redis-py 포함)
├── README.md                     # 프로젝트 문서
├── ai_docs/
│   ├── task_template.md          # 이 템플릿
│   ├── bugfix_template.md        # 버그 수정 템플릿
│   ├── code_review_template.md   # 코드 리뷰 템플릿
│   ├── tasks/                    # 생성된 작업 문서들
│   └── rules/                    # Cursor 규칙들
├── config/
│   ├── __init__.py               # dotenv 자동 로드 설정
│   ├── kafka_config.py           # Kafka 설정 클래스
│   ├── mysql_config.py           # MySQL 설정 클래스
│   ├── redis_config.py           # Redis 설정 클래스
│   ├── spark_config.py           # Spark 설정 클래스
│   └── prometheus_config.py      # Prometheus 설정 클래스
├── scripts/
│   └── generate_sample_data.py   # 샘플 데이터 생성
├── src/
│   ├── kafka/
│   │   ├── producer.py           # 이벤트 시뮬레이터
│   │   └── consumer.py           # Kafka 소비자
│   ├── spark/
│   │   └── streaming.py          # PySpark 스트리밍 (세션 추적)
│   ├── recommendation/
│   │   ├── collaborative_filtering.py  # 협업 필터링
│   │   ├── session_based.py      # 세션 기반 추천
│   │   └── ab_test.py            # A/B 테스트 로직
│   ├── analytics/
│   │   └── session_analyzer.py   # 세션 분석
│   ├── storage/
│   │   ├── mysql_client.py       # MySQL 클라이언트
│   │   └── redis_client.py       # Redis 클라이언트
│   ├── metrics/
│   │   └── prometheus_exporter.py # Prometheus 메트릭
│   ├── api/
│   │   ├── rest_api.py           # REST API 메인
│   │   └── routes/
│   │       ├── recommendations.py # 추천 엔드포인트
│   │       ├── events.py         # 이벤트 엔드포인트
│   │       ├── sessions.py       # 세션 엔드포인트
│   │       └── ab_test.py        # A/B 테스트 엔드포인트
│   └── main.py                   # 메인 실행 파일
├── ui/
│   ├── index.html                # 메인 대시보드
│   ├── css/
│   │   └── style.css             # 스타일시트
│   └── js/
│       ├── dashboard.js          # 대시보드 로직
│       ├── heatmap.js            # 클릭 히트맵
│       ├── recommendations.js    # 추천 표시
│       ├── ab-test-chart.js      # A/B 테스트 차트
│       ├── session-timeline.js   # 세션 타임라인
│       └── api-client.js         # API 통신 유틸
├── tests/                        # 테스트 코드
└── data/                         # 샘플 데이터
    ├── users.json                # 샘플 사용자 데이터
    └── contents.json             # 샘플 콘텐츠 데이터 (영화/드라마/다큐)
```

---

## 12. AI Agent Instructions

### Implementation Workflow
🎯 **MANDATORY PROCESS:**

1. **작업 시작 전:**
   - 관련 파일들을 모두 분석하세요 (`src/`, `config/` 폴더)
   - 기존 코드 패턴과 일관성을 유지하세요
   - 외부 서버 연결 정보를 확인하세요

2. **코드 작성 시:**
   - 모든 외부 연결에 에러 핸들링을 추가하세요
   - 로깅을 철저히 하세요 (`logging` 모듈 사용)
   - 타입 힌트를 사용하세요 (Python 3.9+)
   - Docstring을 작성하세요 (Google 스타일)

3. **완료 후:**
   - 작성한 코드를 테스트하세요
   - `requirements.txt`를 업데이트하세요
   - 이 작업 문서의 체크박스를 업데이트하세요
   - 다음 단계 작업을 제안하세요

4. **외부 서비스 연동 시 주의사항:**
   - Kafka: 항상 재연결 로직 포함
   - MySQL: Connection Pool 사용
   - Redis: TTL 설정 및 Fallback 로직 필수
   - Prometheus: `/metrics` 엔드포인트는 8000번 포트 사용
   - 모든 연결 정보는 `config/` 폴더에서 관리

### Communication Preferences
- 각 단계 완료 시 명확히 보고하세요
- 에러 발생 시 전체 스택 트레이스를 제공하세요
- 대안 솔루션이 있다면 함께 제시하세요
- 코드 변경 사항은 간결하게 요약하세요

### Code Quality Standards
- **Python 스타일:** PEP 8 준수
- **네이밍:** snake_case (함수/변수), PascalCase (클래스)
- **에러 핸들링:** try-except 필수 (외부 연결)
- **로깅 레벨:** DEBUG (개발), INFO (운영)
- **테스트:** 주요 함수는 테스트 케이스 작성

### Configuration Best Practices 🆕
**설정 파일 작성 시 반드시 지켜야 할 원칙:**

#### 🔧 Java의 application.properties와 동일한 방식
Python에서는 `.env` 파일 + `python-dotenv` 라이브러리로 Java의 `application.properties` 방식을 구현합니다.

#### 1. 설정 파일 구조

**`.env.template` (버전 관리 포함 - 설정 예시)**
```bash
# Kafka 설정
KAFKA_BOOTSTRAP_SERVERS=
KAFKA_TOPIC=
KAFKA_CONSUMER_GROUP=

# MySQL 설정
MYSQL_HOST=
MYSQL_PORT=
MYSQL_USER=
MYSQL_PASSWORD=
MYSQL_DATABASE=

# Redis 설정
REDIS_HOST=
REDIS_PORT=
REDIS_DB=
REDIS_PASSWORD=
REDIS_TTL=

# Prometheus 설정
PROMETHEUS_HOST=
PROMETHEUS_PORT=

# Spark 설정
SPARK_APP_NAME=
SPARK_MASTER=local[*]
SPARK_EXECUTOR_MEMORY=2g

# API 설정
API_HOST=localhost
API_PORT=8000
```

**`.env` (버전 관리 제외 - 실제 사용)**
- 개발자가 `.env.template`을 복사하여 생성
- 실제 환경에 맞게 값 수정
- `.gitignore`에 반드시 포함

**`.gitignore`**
```
.env
__pycache__/
*.pyc
.pytest_cache/
```

#### 2. 설정 로딩 구조

**`config/__init__.py` (자동 로딩)**
```python
"""
설정 패키지 초기화
.env 파일을 자동으로 로드합니다.
"""
from dotenv import load_dotenv
import os

# .env 파일 로드 (프로젝트 루트에서)
load_dotenv()

# 설정 클래스 임포트
from .mysql_config import MySQLConfig, get_mysql_config
from .kafka_config import KafkaConfig, get_kafka_config
from .spark_config import SparkConfig, get_spark_config
from .prometheus_config import PrometheusConfig, get_prometheus_config

__all__ = [
    'MySQLConfig', 'get_mysql_config',
    'KafkaConfig', 'get_kafka_config',
    'SparkConfig', 'get_spark_config',
    'PrometheusConfig', 'get_prometheus_config'
]
```

**설정 클래스 예시: `config/mysql_config.py`**
```python
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
    
    @classmethod
    def from_env(cls) -> 'MySQLConfig':
        """환경 변수에서 설정 로드"""
        return cls(
            host=os.getenv('MYSQL_HOST'),
            port=int(os.getenv('MYSQL_PORT', '3306')),  # 표준 포트만 기본값
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD'),
            database=os.getenv('MYSQL_DATABASE')
        )
    
    def validate(self):
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

def get_mysql_config() -> MySQLConfig:
    """싱글톤 패턴으로 MySQL 설정 반환"""
    config = MySQLConfig.from_env()
    config.validate()
    return config
```

#### 3. 사용 예시

**애플리케이션 코드에서 사용**
```python
# src/storage/mysql_client.py
from config import get_mysql_config
import mysql.connector

class MySQLClient:
    def __init__(self):
        # .env 파일에서 자동으로 설정 로드
        self.config = get_mysql_config()
    
    def connect(self):
        return mysql.connector.connect(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            database=self.config.database
        )
```

#### 4. 주요 원칙

**✅ DO (반드시 지킬 것)**
- `.env.template` 파일에 모든 설정 키와 예시값 작성
- `.env` 파일은 `.gitignore`에 추가
- 표준 포트만 기본값 허용 (3306, 9090 등)
- 설정 누락 시 명확한 에러 메시지 제공
- 민감정보는 절대 코드에 하드코딩 금지

**❌ DON'T (하지 말 것)**
- IP 주소, 호스트명, DB명을 기본값으로 제공
- `.env` 파일을 Git에 커밋
- 코드에 비밀번호 하드코딩
- 환경변수 없이 임의의 기본값 사용

#### 5. 프로젝트 시작 시 설정 절차

1. 프로젝트 클론 후 `.env.template`을 `.env`로 복사
   ```bash
   cp .env.template .env
   ```

2. `.env` 파일을 실제 환경에 맞게 수정
   ```bash
   # .env 파일 편집
   notepad .env  # Windows
   ```

3. Python 패키지 설치
   ```bash
   pip install -r requirements.txt
   ```

4. 애플리케이션 실행 (자동으로 .env 로드)
   ```bash
   python src/main.py
   ```

---

## 13. Second-Order Impact Analysis

### Impact Assessment
TODO: 이 작업이 다른 시스템에 미치는 영향 분석

**고려 사항:**
- PySpark 로컬 실행 시 메모리 사용량 (RAM 8GB 이상 권장)
- Kafka Consumer가 중단되면 이벤트 손실 가능성
- MySQL 테이블이 커질 경우 쿼리 성능 저하 (인덱스 최적화 필요)
- Redis 메모리 사용량 (추천 결과 캐싱 크기 제한 필요)
- Prometheus 메트릭 수집 간격과 UI 업데이트 주기 동기화

**성능 우려사항:**
- PySpark 배치 간격 조정 (3초 vs 5초)
- 협업 필터링 계산 주기 (매 1분 vs 5분)
- Redis TTL 설정 (10분 vs 30분)
- UI 리프레시 주기 (3초 vs 5초)
- 세션 윈도우 크기 (3분 vs 5분)

**추천 품질 우려사항:**
- Cold Start 문제 (신규 사용자/상품)
- 데이터 편향 (인기 상품만 추천되는 현상)
- A/B 테스트 샘플 크기 부족 시 통계적 유의성 낮음

**사용자 워크플로우 영향:**
- 시스템 중단 시 진행 중인 세션 분석 손실
- Redis 장애 시 추천 응답 속도 저하 (MySQL Fallback)
- MySQL 연결 실패 시 추천 저장 불가 → 로그 출력으로 대체


