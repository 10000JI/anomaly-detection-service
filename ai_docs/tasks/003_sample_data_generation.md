# Task 003: 샘플 데이터 생성 (Sample Data Generation)

> **작업 번호:** 003  
> **작업명:** Sample Data Generation for Testing  
> **의존성:** Task 002 (MySQL Database Schema Creation)  
> **상태:** ✅ 완료  
> **생성일:** 2026-01-01  
> **완료일:** 2026-01-01

---

## 1. Task Overview

### Task Title
**Title:** 테스트용 샘플 데이터 생성 및 MySQL 데이터베이스 초기화

### Goal Statement
**Goal:** 
실시간 사용자 행동 분석 & 개인화 추천 시스템의 개발 및 테스트를 위해 현실적인 샘플 데이터를 생성하고 MySQL 데이터베이스에 삽입합니다.

**비즈니스 가치:**
- 실제 데이터 구조를 시뮬레이션하여 시스템 동작 검증
- 추천 알고리즘 개발 및 테스트에 필요한 기반 데이터 제공
- A/B 테스트 및 성능 측정을 위한 충분한 데이터 확보

---

## 2. Current State Analysis

### 기존 상태
- ✅ MySQL 데이터베이스 스키마 생성 완료 (7개 테이블)
- ✅ MySQL 클라이언트 구현 완료
- ❌ 샘플 사용자 데이터 없음
- ❌ 샘플 콘텐츠 데이터 없음
- ❌ 데이터 생성 스크립트 없음

### 데이터베이스 정보
- **Host:** 192.168.150.110
- **Port:** 3306
- **Database:** cursor_practice
- **Tables:** user_profiles, contents (비어있음)

---

## 3. Technical Requirements

### Functional Requirements
- 시스템은 100명의 사용자 프로필 데이터를 생성한다
- 시스템은 1,000개의 콘텐츠 데이터를 생성한다 (영화 600, 드라마 300, 다큐멘터리 100)
- 사용자 데이터는 다양한 세그먼트로 분류된다 (VIP, Regular, New)
- 콘텐츠 데이터는 현실적인 메타데이터를 포함한다 (장르, 평점, 재생시간 등)
- 생성된 데이터는 MySQL 데이터베이스에 자동으로 삽입된다
- 데이터 생성 과정은 로깅되고 검증된다

### Non-Functional Requirements
- **Performance:** 전체 데이터 생성 및 삽입 시간 < 30초
- **Data Quality:** 현실적이고 일관성 있는 데이터
- **Reproducibility:** 동일한 시드값으로 동일한 데이터 재생성 가능
- **Extensibility:** 추가 데이터 생성 시 쉽게 확장 가능

---

## 4. Sample Data Specifications

### 4.1. 사용자 프로필 데이터 (100명)

**테이블:** `user_profiles`

**분포:**
- VIP 세그먼트: 20명 (20%)
- Regular 세그먼트: 50명 (50%)
- New 세그먼트: 30명 (30%)

**데이터 필드:**
```python
{
    "user_id": "user-00001",              # user-00001 ~ user-00100
    "user_segment": "VIP",                # VIP, Regular, New
    "signup_date": "2024-01-15",          # 2023-01-01 ~ 2025-12-31
    "total_purchases": 45,                # VIP: 30-100, Regular: 5-30, New: 0-5
    "total_spent": 1250.50,               # VIP: 1000-5000, Regular: 100-1000, New: 0-100
    "favorite_categories": ["액션", "SF"] # 1-5개 랜덤 장르
}
```

**세그먼트별 특성:**
- **VIP**: 높은 구매력, 다양한 선호 장르, 오래된 가입일
- **Regular**: 중간 구매력, 2-3개 선호 장르, 중간 가입일
- **New**: 낮은/없는 구매 이력, 1-2개 선호 장르, 최근 가입일

---

### 4.2. 콘텐츠 데이터 (1,000개)

**테이블:** `contents`

**분포:**
- 영화 (movie): 600개 (60%)
- 드라마 (series): 300개 (30%)
- 다큐멘터리 (documentary): 100개 (10%)

**데이터 필드:**
```python
{
    "content_id": "movie-00001",          # movie-xxxxx, series-xxxxx, doc-xxxxx
    "title": "어벤져스: 엔드게임",
    "content_type": "movie",              # movie, series, documentary
    "genre": "액션",                      # 주 장르
    "sub_genre": "SF",                    # 서브 장르
    "duration_minutes": 180,              # 영화: 80-200, 드라마: 30-60, 다큐: 40-120
    "release_year": 2019,                 # 1990-2025
    "rating": 4.5,                        # 1.0-5.0
    "review_count": 15234                 # 10-50000
}
```

**장르 분포:**
- **영화**: 액션(30%), 코미디(20%), 드라마(20%), SF(15%), 공포(10%), 로맨스(5%)
- **드라마**: 로맨스(30%), 드라마(25%), 코미디(20%), 스릴러(15%), 판타지(10%)
- **다큐멘터리**: 자연(30%), 역사(25%), 과학(20%), 사회(15%), 예술(10%)

---

## 5. Implementation Plan

### Phase 1: 데이터 생성 유틸리티 구현
**Tasks:**
- [ ] `scripts/generate_sample_data.py` 스크립트 생성
- [ ] 사용자 프로필 생성 함수
- [ ] 콘텐츠 데이터 생성 함수
- [ ] 한글 이름/제목 생성 로직

### Phase 2: MySQL 데이터 삽입
**Tasks:**
- [ ] 배치 삽입 최적화 (한 번에 여러 레코드)
- [ ] 중복 확인 로직
- [ ] 트랜잭션 관리

### Phase 3: 검증 및 테스트
**Tasks:**
- [ ] 데이터 삽입 확인
- [ ] 데이터 일관성 검증
- [ ] 통계 정보 출력

---

## 6. Data Generation Logic

### 6.1. 사용자 프로필 생성

```python
def generate_user_profiles(count: int = 100) -> List[Dict]:
    """
    사용자 프로필 데이터 생성
    
    Args:
        count: 생성할 사용자 수
        
    Returns:
        사용자 프로필 리스트
    """
    segments = {
        'VIP': 20,      # 20명
        'Regular': 50,  # 50명
        'New': 30       # 30명
    }
    
    genres = ['액션', '코미디', '드라마', 'SF', '공포', '로맨스', 
              '스릴러', '판타지', '애니메이션', '범죄']
    
    profiles = []
    user_id = 1
    
    for segment, segment_count in segments.items():
        for _ in range(segment_count):
            if segment == 'VIP':
                total_purchases = random.randint(30, 100)
                total_spent = round(random.uniform(1000, 5000), 2)
                favorite_count = random.randint(3, 5)
                signup_date = generate_date(2020, 2023)
            elif segment == 'Regular':
                total_purchases = random.randint(5, 30)
                total_spent = round(random.uniform(100, 1000), 2)
                favorite_count = random.randint(2, 3)
                signup_date = generate_date(2022, 2024)
            else:  # New
                total_purchases = random.randint(0, 5)
                total_spent = round(random.uniform(0, 100), 2)
                favorite_count = random.randint(1, 2)
                signup_date = generate_date(2024, 2025)
            
            profile = {
                'user_id': f'user-{user_id:05d}',
                'user_segment': segment,
                'signup_date': signup_date,
                'total_purchases': total_purchases,
                'total_spent': total_spent,
                'favorite_categories': random.sample(genres, favorite_count)
            }
            
            profiles.append(profile)
            user_id += 1
    
    return profiles
```

### 6.2. 콘텐츠 데이터 생성

```python
def generate_contents(
    movie_count: int = 600,
    series_count: int = 300,
    documentary_count: int = 100
) -> List[Dict]:
    """
    콘텐츠 데이터 생성
    
    Args:
        movie_count: 영화 개수
        series_count: 드라마 개수
        documentary_count: 다큐멘터리 개수
        
    Returns:
        콘텐츠 리스트
    """
    contents = []
    
    # 영화 생성
    contents.extend(generate_movies(movie_count))
    
    # 드라마 생성
    contents.extend(generate_series(series_count))
    
    # 다큐멘터리 생성
    contents.extend(generate_documentaries(documentary_count))
    
    return contents
```

---

## 7. Implementation Details

### 파일 구조

```
scripts/
├── generate_sample_data.py      # 메인 실행 스크립트
└── data_generators/
    ├── __init__.py
    ├── user_generator.py         # 사용자 데이터 생성
    ├── content_generator.py      # 콘텐츠 데이터 생성
    └── data_templates.py         # 템플릿 및 상수

data/
├── users.json                    # 생성된 사용자 데이터 (백업)
└── contents.json                 # 생성된 콘텐츠 데이터 (백업)
```

### 주요 함수

**UserGenerator 클래스:**
```python
class UserGenerator:
    def __init__(self, count: int = 100):
        self.count = count
        
    def generate(self) -> List[Dict]:
        """사용자 프로필 생성"""
        
    def save_to_json(self, filepath: str):
        """JSON 파일로 저장"""
        
    def insert_to_mysql(self, mysql_client: MySQLClient):
        """MySQL에 삽입"""
```

**ContentGenerator 클래스:**
```python
class ContentGenerator:
    def __init__(
        self, 
        movie_count: int = 600,
        series_count: int = 300,
        documentary_count: int = 100
    ):
        self.movie_count = movie_count
        self.series_count = series_count
        self.documentary_count = documentary_count
        
    def generate(self) -> List[Dict]:
        """콘텐츠 데이터 생성"""
        
    def save_to_json(self, filepath: str):
        """JSON 파일로 저장"""
        
    def insert_to_mysql(self, mysql_client: MySQLClient):
        """MySQL에 삽입"""
```

---

## 8. Success Criteria

### 기능 완성도
- ✅ 100명의 사용자 프로필 생성
- ✅ 1,000개의 콘텐츠 생성 (영화 600, 드라마 300, 다큐 100)
- ✅ MySQL에 모든 데이터 삽입 완료
- ✅ 데이터 일관성 검증 통과

### 데이터 품질
- ✅ 세그먼트별 분포 정확 (VIP 20%, Regular 50%, New 30%)
- ✅ 콘텐츠 타입별 분포 정확 (영화 60%, 드라마 30%, 다큐 10%)
- ✅ 현실적인 메타데이터 (평점, 재생시간, 연도 등)
- ✅ 한글 이름/제목 포함

### 성능
- ✅ 전체 생성 및 삽입 시간 < 30초
- ✅ 배치 삽입으로 성능 최적화
- ✅ 메모리 사용량 < 500MB

---

## 9. Progress Tracking

### Phase 1: 데이터 생성 유틸리티 구현 ✅
- [x] UserGenerator 클래스 구현
- [x] ContentGenerator 클래스 구현
- [x] 데이터 템플릿 정의
- [x] JSON 저장 기능

### Phase 2: MySQL 데이터 삽입 ✅
- [x] 배치 삽입 로직
- [x] 트랜잭션 관리
- [x] 중복 확인

### Phase 3: 검증 및 테스트 ✅
- [x] 데이터 삽입 확인
- [x] 통계 출력
- [x] 테스트 실행

---

## 10. Execution Commands

```bash
# 샘플 데이터 생성 및 MySQL 삽입
python scripts/generate_sample_data.py

# JSON 파일로만 저장 (MySQL 삽입 안 함)
python scripts/generate_sample_data.py --json-only

# 특정 개수로 생성
python scripts/generate_sample_data.py --users 50 --movies 300 --series 150 --docs 50
```

---

## 11. Next Steps

이 작업 완료 후:
1. **Task 004:** Kafka Producer 구현 (이벤트 시뮬레이터)
2. **Task 005:** Kafka Consumer 및 PySpark 스트리밍 구현
3. **Task 006:** 추천 알고리즘 구현

---

## 12. Execution Summary

### 실행 결과
**실행 일시:** 2026-01-01 21:57:31 ~ 21:57:55

**사용자 프로필 생성 결과:**
- ✅ 총 100명 생성
- ✅ VIP: 20명 (20.0%)
- ✅ Regular: 50명 (50.0%)
- ✅ New: 30명 (30.0%)
- 📊 평균 구매 횟수: 24.16회
- 📊 평균 구매 금액: 823.73원
- 📊 총 구매 금액: 82,373.30원

**콘텐츠 데이터 생성 결과:**
- ✅ 총 1,000개 생성
- ✅ 영화 (movie): 600개 (60.0%)
- ✅ 드라마 (series): 300개 (30.0%)
- ✅ 다큐멘터리 (documentary): 100개 (10.0%)
- 📊 평균 평점: 3.2/5.0
- 📊 평균 재생시간: 105.3분

**상위 5개 장르:**
1. 드라마: 207개
2. 코미디: 195개
3. 액션: 162개
4. 로맨스: 120개
5. SF: 95개

**MySQL 데이터베이스 검증:**
- ✅ user_profiles 테이블: 100명 삽입 완료
  - New: 30명
  - Regular: 50명
  - VIP: 20명
- ✅ contents 테이블: 1,000개 삽입 완료
  - movie: 600개
  - series: 300개
  - documentary: 100개

**생성된 파일:**
```
scripts/data_generators/
├── __init__.py                 ✅ 생성 (7줄)
├── data_templates.py           ✅ 생성 (106줄)
├── user_generator.py           ✅ 생성 (244줄)
└── content_generator.py        ✅ 생성 (250줄)

scripts/
└── generate_sample_data.py     ✅ 생성 (358줄)

data/
├── users.json                  ✅ 생성 (100명 데이터)
└── contents.json               ✅ 생성 (1,000개 데이터)

ai_docs/tasks/
└── 003_sample_data_generation.md  ✅ 작업 문서

sample_data_20260101_215731.log   ✅ 실행 로그
```

**성능 지표:**
- 전체 실행 시간: 약 23.5초 (목표: 30초 이하) ✅
- 사용자 데이터 생성: 약 0.003초 ✅
- 콘텐츠 데이터 생성: 약 0.022초 ✅
- MySQL 삽입 시간: 약 23초 ✅
- 메모리 사용량: < 100MB ✅

### 검증 완료 항목
- ✅ 세그먼트별 사용자 분포 정확 (VIP 20%, Regular 50%, New 30%)
- ✅ 콘텐츠 타입별 분포 정확 (영화 60%, 드라마 30%, 다큐 10%)
- ✅ 모든 데이터 MySQL에 정상 삽입
- ✅ JSON 백업 파일 생성
- ✅ 데이터 일관성 검증 통과
- ✅ 참조 무결성 확인 (FK 관계)

---

**작업 시작일:** 2026-01-01  
**작업 완료일:** 2026-01-01  
**담당자:** AI Agent  
**상태:** ✅ 완료


