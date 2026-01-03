# 실시간 사용자 행동 분석 & 개인화 추천 시스템

> Real-Time User Behavior Analysis & Personalized Recommendation System

협업 필터링과 세션 분석을 활용한 실시간 콘텐츠 추천 시스템 - Netflix 스타일 추천 엔진 구현

## 📋 목차

- [프로젝트 개요](#-프로젝트-개요)
- [시스템 아키텍처](#-시스템-아키텍처)
- [기술 스택](#-기술-스택)
- [시작하기](#-시작하기)
- [프로젝트 구조](#-프로젝트-구조)
- [설정 가이드](#-설정-가이드)
- [개발 가이드](#-개발-가이드)

## 🎯 프로젝트 개요

### 핵심 목표

1. **실시간 행동 추적**: 사용자의 클릭, 시청, 평가 이벤트를 실시간으로 수집하고 세션화
2. **지능형 추천**: 협업 필터링과 세션 패턴 분석을 통한 개인화 콘텐츠 추천
3. **고속 응답**: Redis 캐싱을 활용하여 추천 결과를 100ms 이내 응답
4. **A/B 테스트**: 실시간으로 추천 알고리즘 성능을 비교하고 최적화

### 주요 기능

- Kafka 클러스터로부터 실시간 사용자 이벤트 수집
- PySpark 스트리밍 기반 3분 윈도우 세션 추적
- 협업 필터링 알고리즘 기반 개인화 추천 생성
- Redis를 활용한 초고속 추천 결과 캐싱 (TTL 10분)
- MySQL 데이터베이스에 이벤트 및 추천 결과 저장
- A/B 테스트 그룹별 성능 비교 및 통계 분석
- Prometheus 메트릭 노출 및 모니터링
- REST API를 통한 추천 조회 및 시스템 제어
- 웹 UI를 통한 실시간 시청 히트맵 및 추천 결과 시각화

## 🏗️ 시스템 아키텍처

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│   Kafka     │─────▶│   PySpark    │─────▶│    MySQL    │
│  Cluster    │      │  Streaming   │      │  Database   │
│ (External)  │      │   (Local)    │      │ (External)  │
│             │      │              │      │             │
│  Events:    │      │  - Session   │      │  - Events   │
│  - Click    │      │    Tracking  │      │  - Profiles │
│  - Watch    │      │  - 3m Window │      │  - Contents │
│  - Rating   │      │              │      │             │
└─────────────┘      └──────┬───────┘      └─────────────┘
                            │
                            │ ML Pipeline
                            ▼
                     ┌──────────────┐
                     │ Collaborative│
                     │  Filtering & │
                     │   Session    │
                     │   Analysis   │
                     └──────┬───────┘
                            │
        ┌───────────────────┴───────────────────┐
        │                                       │
  ┌─────▼──────┐      ┌──────────┐      ┌─────▼──────┐
  │   Redis    │      │Prometheus│      │  REST API  │
  │   Cache    │      │  Metrics │      │  + Web UI  │
  │ (External) │      │(External)│      │   (Local)  │
  │            │      │          │      │            │
  │ 추천 결과  │      │ 시스템   │      │ - 추천 조회│
  │  캐싱      │      │  메트릭  │      │ - A/B 비교 │
  │ (TTL 10m)  │      │          │      │ - 히트맵   │
  └────────────┘      └──────────┘      └────────────┘
```

## 🛠️ 기술 스택

### 핵심 기술

- **Language**: Python 3.9.6
- **Stream Processing**: Apache Spark 3.4.1 (PySpark - Local)
- **Message Broker**: Apache Kafka 3.8.1 (External Cluster)
- **Database**: MySQL (External Server)
- **Cache**: Redis (External Server)
- **Monitoring**: Prometheus (External Server)
- **ML Algorithms**: Collaborative Filtering, Session Analysis (scikit-learn, pandas)
- **API Framework**: FastAPI
- **UI**: Vanilla JavaScript

### 주요 라이브러리

- `pyspark`: 스트림 처리 및 세션 추적
- `kafka-python`: Kafka 클라이언트
- `mysql-connector-python`: MySQL 연결
- `redis-py`: Redis 캐싱
- `scikit-learn`: 협업 필터링 알고리즘
- `prometheus-client`: 메트릭 노출
- `fastapi`: REST API
- `python-dotenv`: 환경 변수 관리

## 🚀 시작하기

### 사전 요구사항

1. **Python 3.9.6 이상** 설치
2. **외부 서비스 접근 권한**:
   - Kafka Cluster: `192.168.150.115:9092,192.168.150.120:9092,192.168.150.125:9092`
   - MySQL Server: `192.168.150.110:3306`
   - Redis Server: `192.168.150.110:6379`
   - Prometheus Server: `192.168.150.110:19090`

### 설치 방법

#### 1. 저장소 클론

```bash
git clone <repository-url>
cd anomaly-detection-service
```

#### 2. 가상 환경 생성 (권장)

**Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

**Linux/Mac:**
```bash
python -m venv venv
source venv/bin/activate
```

#### 3. 의존성 설치

```bash
pip install -r requirements.txt
```

#### 4. 환경 변수 설정

```bash
# Windows
copy .env.template .env

# Linux/Mac
cp .env.template .env
```

`.env` 파일을 열어 실제 연결 정보를 확인/수정하세요.

#### 5. 데이터베이스 스키마 생성

```bash
python scripts/run_migrations.py
```

MySQL 데이터베이스에 필요한 7개 테이블을 자동으로 생성합니다:
- `user_profiles`: 사용자 프로필
- `contents`: 콘텐츠 메타데이터
- `user_events`: 사용자 이벤트 히스토리
- `user_sessions`: 세션 분석 결과
- `recommendations`: 추천 결과
- `ab_test_groups`: A/B 테스트 그룹
- `ab_test_metrics`: A/B 테스트 성능 지표

#### 6. 스키마 생성 검증

```bash
python tests/test_db_schema.py
```

데이터베이스 스키마가 올바르게 생성되었는지 검증합니다 (11개 테스트).

#### 7. 샘플 데이터 생성

```bash
python scripts/generate_sample_data.py
```

테스트용 샘플 데이터를 생성하고 MySQL에 삽입합니다:
- 사용자 프로필: 100명 (VIP 20, Regular 50, New 30)
- 콘텐츠: 1,000개 (영화 600, 드라마 300, 다큐 100)
- JSON 백업 파일: `data/users.json`, `data/contents.json`

#### 8. 연결 테스트

```bash
python tests/test_connections.py
```

모든 외부 서비스(Kafka, MySQL, Redis, Prometheus)와 PySpark 로컬 세션 연결을 확인합니다.

#### 6. 데이터베이스 스키마 생성

```bash
python scripts/run_migrations.py
```

7개 테이블 자동 생성 후 `python tests/test_db_schema.py`로 검증합니다.

#### 7. 샘플 데이터 생성

```bash
python scripts/generate_sample_data.py
```

사용자 100명, 콘텐츠 1,000개를 생성하여 MySQL에 삽입합니다.

### 실행 방법

```bash
# API 서버 시작 (개발 중)
python src/main.py
```

## 📁 프로젝트 구조

```
recommendation-system/
├── .env                          # 🔒 실제 설정값 (gitignore)
├── .env.template                 # 📝 설정 예시
├── .gitignore                    # Git 제외 목록
├── requirements.txt              # Python 패키지
├── README.md                     # 프로젝트 문서
├── ai_docs/                      # AI 작업 문서
│   ├── task_template.md
│   ├── bugfix_template.md
│   ├── code_review_template.md
│   └── tasks/
│       ├── 001_setup_configuration_files.md        # ✅ 완료
│       ├── 002_mysql_database_schema_creation.md   # ✅ 완료
│       └── 003_sample_data_generation.md           # ✅ 완료
├── config/                       # 설정 모듈
│   ├── __init__.py               # dotenv 자동 로드
│   ├── kafka_config.py           # Kafka 설정
│   ├── mysql_config.py           # MySQL 설정
│   ├── redis_config.py           # Redis 설정
│   ├── spark_config.py           # Spark 설정
│   └── prometheus_config.py      # Prometheus 설정
├── src/                          # 소스 코드
│   ├── kafka/
│   │   ├── producer.py           # (예정) 이벤트 시뮬레이터
│   │   └── consumer.py           # (예정) Kafka 소비자
│   ├── spark/
│   │   └── streaming.py          # (예정) PySpark 스트리밍
│   ├── recommendation/
│   │   ├── collaborative_filtering.py   # (예정) 협업 필터링
│   │   ├── session_based.py      # (예정) 세션 기반 추천
│   │   └── ab_test.py            # (예정) A/B 테스트
│   ├── storage/
│   │   ├── mysql_client.py       # ✅ MySQL 클라이언트
│   │   ├── migrations.py         # ✅ DB 마이그레이션
│   │   └── redis_client.py       # (예정) Redis 클라이언트
│   ├── metrics/
│   │   └── prometheus_exporter.py # (예정) Prometheus
│   ├── api/
│   │   └── rest_api.py           # (예정) REST API
│   └── main.py                   # (예정) 메인 실행
├── scripts/                      # 실행 스크립트
│   └── run_migrations.py         # ✅ DB 마이그레이션 실행
├── tests/                        # 테스트 코드
│   ├── test_connections.py       # 연결 테스트
│   └── test_db_schema.py         # ✅ DB 스키마 테스트
├── ui/                           # 웹 UI
│   ├── index.html                # (예정) 메인 페이지
│   ├── css/
│   └── js/
└── data/                         # 샘플 데이터
    ├── users.json                # (예정) 샘플 사용자
    └── contents.json             # (예정) 샘플 콘텐츠
```

## ⚙️ 설정 가이드

### 환경 변수 구조

시스템은 `.env` 파일에서 모든 설정을 로드합니다. (Java의 `application.properties`와 동일)

#### Kafka 설정

```bash
KAFKA_BOOTSTRAP_SERVERS=192.168.150.115:9092,192.168.150.120:9092,192.168.150.125:9092
KAFKA_TOPIC=user-events-topic
KAFKA_CONSUMER_GROUP=recommendation-engine-group
KAFKA_AUTO_OFFSET_RESET=earliest
KAFKA_ENABLE_AUTO_COMMIT=false
```

#### MySQL 설정

```bash
MYSQL_HOST=192.168.150.110
MYSQL_PORT=3306
MYSQL_USER=didim
MYSQL_PASSWORD=fpemdnemzpdl123$
MYSQL_DATABASE=cursor_practice
MYSQL_POOL_SIZE=5
```

#### Redis 설정

```bash
REDIS_HOST=192.168.150.110
REDIS_PORT=6379
REDIS_DB=0
REDIS_PASSWORD=
REDIS_TTL=600
```

#### Prometheus 설정

```bash
PROMETHEUS_HOST=192.168.150.110
PROMETHEUS_PORT=19090
```

#### PySpark 설정

```bash
SPARK_APP_NAME=RecommendationSystem
SPARK_MASTER=local[*]
SPARK_EXECUTOR_MEMORY=2g
SPARK_DRIVER_MEMORY=1g
SPARK_LOG_LEVEL=WARN
```

### 설정 클래스 사용 방법

```python
from config import get_mysql_config, get_kafka_config, get_redis_config

# 설정 로드 (자동 검증)
mysql_config = get_mysql_config()
kafka_config = get_kafka_config()
redis_config = get_redis_config()

# 설정 사용
print(mysql_config.host)  # 192.168.150.110
servers = kafka_config.get_bootstrap_servers_list()
redis_url = redis_config.get_redis_url()
```

## 👨‍💻 개발 가이드

### 코드 스타일

- **Python 스타일**: PEP 8 준수
- **네이밍**: `snake_case` (함수/변수), `PascalCase` (클래스)
- **타입 힌트**: 모든 함수에 타입 힌트 사용
- **Docstring**: Google 스타일

### 에러 핸들링

```python
try:
    # 외부 서비스 연결
    connection = connect_to_service()
except ConnectionError as e:
    logger.error(f"연결 실패: {str(e)}")
    # 재시도 로직
```

### 로깅

```python
import logging

logger = logging.getLogger(__name__)
logger.info("시스템 시작")
logger.error("에러 발생", exc_info=True)
```

### 테스트 실행

```bash
# 연결 테스트 (Kafka, MySQL, Redis, Prometheus)
python tests/test_connections.py

# 데이터베이스 스키마 테스트 (11개 테스트)
python tests/test_db_schema.py

# 모든 테스트 실행 (pytest)
pytest

# 특정 파일 테스트
pytest tests/test_connections.py

# 커버리지 리포트
pytest --cov=src --cov-report=html
```

### 데이터베이스 관리

```bash
# 1. 데이터베이스 스키마 생성 (7개 테이블)
python scripts/run_migrations.py

# 2. 샘플 데이터 생성 및 삽입
python scripts/generate_sample_data.py

# 3. JSON 파일로만 저장 (MySQL 삽입 안 함)
python scripts/generate_sample_data.py --json-only

# 4. 커스텀 개수로 생성
python scripts/generate_sample_data.py --users 50 --movies 300

# 생성된 로그 파일
# - migration_YYYYMMDD_HHMMSS.log (스키마)
# - sample_data_YYYYMMDD_HHMMSS.log (데이터)
```

## 📝 작업 진행 상황

### ✅ 완료된 작업

- [x] **Task 001: 외부 서비스 설정 파일 구성** (2025-12-29)
  - `.env` 환경 변수 관리
  - 5개 설정 클래스 구현 (Kafka, MySQL, Redis, Spark, Prometheus)
  - 연결 테스트 스크립트

- [x] **Task 002: MySQL 데이터베이스 스키마 생성** (2026-01-01)
  - MySQL 클라이언트 구현 (Connection Pool, 재시도 로직)
  - 7개 테이블 마이그레이션 구현
  - 테스트 코드 작성 (11개 테스트 통과)
  - 스키마 생성 스크립트 (`scripts/run_migrations.py`)
  - **생성된 테이블**: `user_profiles`, `contents`, `user_events`, `user_sessions`, `recommendations`, `ab_test_groups`, `ab_test_metrics`

- [x] **Task 003: 샘플 데이터 생성** (2026-01-01)
  - 사용자 프로필 생성기 구현 (`UserGenerator`)
  - 콘텐츠 데이터 생성기 구현 (`ContentGenerator`)
  - 데이터 템플릿 및 상수 정의
  - 샘플 데이터 생성 스크립트 (`scripts/generate_sample_data.py`)
  - **생성 완료**: 사용자 100명, 콘텐츠 1,000개 (영화 600, 드라마 300, 다큐 100)
  - JSON 백업 파일: `data/users.json`, `data/contents.json`

### 🚧 진행 중인 작업

- [ ] Task 004: Kafka Producer 구현 (이벤트 시뮬레이터)
- [ ] Task 005: Kafka Consumer 및 PySpark 스트리밍 구현
- [ ] Task 006: Redis 클라이언트 및 추천 알고리즘 구현
- [ ] Task 007: REST API 구현
- [ ] Task 008: 웹 UI 개발

## 🎨 주요 기능 상세

### 1. 실시간 이벤트 수집

- Kafka로부터 초당 5,000건 이상의 이벤트 수집
- 이벤트 타입: 클릭, 시청, 시청 완료, 평가, 찜하기

### 2. 세션 추적

- 3분 윈도우 기반 사용자 세션 자동 추적
- 세션 내 이벤트 집계 및 패턴 분석

### 3. 개인화 추천

- **협업 필터링**: 유사 사용자 기반 추천
- **세션 기반**: 현재 세션 패턴 기반 실시간 추천
- **A/B 테스트**: 다중 알고리즘 성능 비교

### 4. 고속 캐싱

- Redis TTL 10분 설정으로 추천 결과 캐싱
- 응답 시간 100ms 이하 달성
- Redis 장애 시 MySQL Fallback

### 5. 실시간 모니터링

- Prometheus 메트릭 노출
- 15개 이상의 핵심 지표 추적
- 실시간 대시보드 제공

## 📄 라이선스

이 프로젝트는 학습 목적으로 개발되었습니다.

## 🤝 기여하기

버그 리포트 및 기능 제안은 이슈로 등록해주세요.

## 📧 문의

프로젝트에 대한 문의사항이 있으시면 이슈를 생성해주세요.

---

**Last Updated**: 2026-01-01  
**Version**: 0.3.0 (MVP - Data Ready)  
**Status**: 
- ✅ Database Schema (7 tables)
- ✅ Sample Data (100 users, 1,000 contents)
- 🚧 Event Streaming (Kafka + PySpark)

**Project**: Real-Time User Behavior Analysis & Personalized Recommendation System
