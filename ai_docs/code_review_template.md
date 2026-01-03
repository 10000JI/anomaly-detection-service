# AI Code Review Template - Real-Time Log Anomaly Detection System

> **프로젝트:** 실시간 로그 기반 이상 징후 감지 시스템  
> **환경:** Windows Local (Cursor IDE)  
> **템플릿 타입:** 코드 품질 검사 및 리뷰

---

## 1. Review Overview

### Review Title
**Title:** [예: "Kafka Consumer 구현 코드 리뷰"]

### Review Scope
**검토 대상:**
- [ ] 신규 기능 (`feature/` 브랜치)
- [ ] 버그 수정 (`bugfix/` 브랜치)
- [ ] 리팩토링 (`refactor/` 브랜치)
- [ ] 성능 최적화 (`optimize/` 브랜치)

**파일 목록:**
- `[파일 1]` - [변경 내용 요약]
- `[파일 2]` - [변경 내용 요약]
- `[파일 3]` - [변경 내용 요약]

### Review Context
**리뷰 배경:**
[왜 이 코드를 작성했는지, 어떤 문제를 해결하는지]

---

## 2. Code Quality Assessment

### 2.1 Code Structure & Organization

#### Architecture Compliance
**프로젝트 구조 준수:**
- [ ] ✅ 올바른 디렉토리에 파일 배치
- [ ] ✅ 모듈 간 의존성이 명확함
- [ ] ✅ 레이어 분리 원칙 준수 (config/src/ui)

**발견사항:**
- [예: "API 로직이 storage 레이어에 섞여 있음"]

#### Code Organization
**코드 구성:**
- [ ] ✅ 함수/클래스가 단일 책임 원칙을 따름
- [ ] ✅ 적절한 추상화 레벨 유지
- [ ] ✅ 코드 중복이 없음

**발견사항:**
- [예: "동일한 에러 핸들링 로직이 3곳에 중복됨"]

---

### 2.2 Code Readability

#### Naming Conventions
**네이밍 규칙:**
- [ ] ✅ snake_case 사용 (함수/변수)
- [ ] ✅ PascalCase 사용 (클래스)
- [ ] ✅ UPPER_CASE 사용 (상수)
- [ ] ✅ 의미있는 변수명 사용

**개선 필요:**
```python
# ❌ 나쁜 예
def f(x, y):
    return x + y

# ✅ 좋은 예
def calculate_total_messages(processed_count: int, failed_count: int) -> int:
    return processed_count + failed_count
```

#### Comments & Documentation
**주석 및 문서화:**
- [ ] ✅ Docstring 작성 (Google 스타일)
- [ ] ✅ 복잡한 로직에 설명 주석 추가
- [ ] ✅ TODO/FIXME가 적절히 사용됨
- [ ] ✅ 타입 힌트 사용

**Docstring 예시:**
```python
def process_log_message(message: dict) -> AnomalyAlert:
    """로그 메시지를 처리하고 이상 징후를 감지합니다.

    Args:
        message: Kafka로부터 수신한 JSON 형식의 로그 메시지

    Returns:
        AnomalyAlert: 감지된 이상 징후 객체

    Raises:
        ValueError: message가 필수 필드를 포함하지 않을 때
        ConnectionError: MySQL 연결 실패 시
    """
    pass
```

---

### 2.3 Error Handling & Logging

#### Exception Handling
**예외 처리:**
- [ ] ✅ 모든 외부 연결에 try-except 사용
- [ ] ✅ 구체적인 예외 타입 catch
- [ ] ✅ 예외 메시지가 명확함
- [ ] ✅ 필요시 재시도 로직 구현

**개선 필요:**
```python
# ❌ 나쁜 예
try:
    kafka_consumer.poll()
except:
    pass

# ✅ 좋은 예
try:
    kafka_consumer.poll(timeout_ms=1000)
except KafkaConnectionError as e:
    logger.error(f"Kafka connection failed: {e}")
    self._reconnect_with_backoff()
except Exception as e:
    logger.exception(f"Unexpected error during polling: {e}")
    raise
```

#### Logging Practices
**로깅 실무:**
- [ ] ✅ 적절한 로그 레벨 사용 (DEBUG/INFO/WARNING/ERROR)
- [ ] ✅ 충분한 컨텍스트 정보 포함
- [ ] ✅ 민감정보 마스킹 (비밀번호, API 키)
- [ ] ✅ 구조화된 로깅 (JSON 형식 권장)

**로깅 예시:**
```python
# ✅ 좋은 로깅
logger.info(
    "Kafka message processed",
    extra={
        "topic": topic_name,
        "partition": partition,
        "offset": offset,
        "processing_time_ms": elapsed_ms
    }
)

# ❌ 나쁜 로깅
logger.info("Message processed")
```

---

### 2.4 Performance & Efficiency

#### Resource Management
**리소스 관리:**
- [ ] ✅ 연결 풀 사용 (MySQL, Kafka)
- [ ] ✅ with 문으로 리소스 자동 해제
- [ ] ✅ 메모리 누수 없음
- [ ] ✅ 불필요한 객체 생성 최소화

**개선 필요:**
```python
# ❌ 나쁜 예
def get_data():
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM table")
    return cursor.fetchall()
    # 연결이 닫히지 않음!

# ✅ 좋은 예
def get_data():
    with mysql.connector.connect(**config) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM table")
            return cursor.fetchall()
```

#### Algorithm Efficiency
**알고리즘 효율성:**
- [ ] ✅ 시간 복잡도가 적절함
- [ ] ✅ 불필요한 반복문 없음
- [ ] ✅ 적절한 자료구조 사용

**발견사항:**
- [예: "리스트 대신 set을 사용하면 O(n)→O(1) 개선 가능"]

---

### 2.5 Security

#### Data Security
**데이터 보안:**
- [ ] ✅ 비밀번호/API 키가 코드에 하드코딩되지 않음
- [ ] ✅ 환경 변수 또는 .env 파일 사용
- [ ] ✅ .env 파일이 .gitignore에 포함됨
- [ ] ✅ SQL Injection 방지 (파라미터화된 쿼리)

**보안 취약점:**
```python
# ❌ 위험한 코드
query = f"SELECT * FROM users WHERE id = {user_id}"  # SQL Injection 위험

# ✅ 안전한 코드
query = "SELECT * FROM users WHERE id = %s"
cursor.execute(query, (user_id,))
```

#### Input Validation
**입력 검증:**
- [ ] ✅ 사용자 입력 검증
- [ ] ✅ 타입 체크
- [ ] ✅ 범위 검증

---

### 2.6 Testing

#### Test Coverage
**테스트 커버리지:**
- [ ] ✅ 핵심 함수에 단위 테스트 존재
- [ ] ✅ Edge case 테스트
- [ ] ✅ 에러 시나리오 테스트

**추가 필요 테스트:**
- [예: "Kafka 재연결 로직 테스트"]
- [예: "ML 알고리즘 정확도 테스트"]

#### Testability
**테스트 가능성:**
- [ ] ✅ 함수가 작고 독립적임
- [ ] ✅ 의존성 주입 가능
- [ ] ✅ Mock/Stub 작성 가능

---

## 3. Project-Specific Checks

### 3.1 External Service Integration

#### Kafka Integration
**Kafka 연동 체크리스트:**
- [ ] ✅ 자동 재연결 로직 구현
- [ ] ✅ Consumer offset 관리
- [ ] ✅ 메시지 손실 방지 (auto_commit=False)
- [ ] ✅ Graceful shutdown 구현

**코드 예시:**
```python
# 재연결 로직 예시
def _reconnect_with_backoff(self):
    for attempt in range(self.max_retries):
        try:
            self.consumer.close()
            self.consumer = KafkaConsumer(**self.config)
            logger.info("Kafka reconnection successful")
            return
        except Exception as e:
            wait_time = 2 ** attempt
            logger.warning(f"Reconnection attempt {attempt+1} failed, waiting {wait_time}s")
            time.sleep(wait_time)
```

#### MySQL Integration
**MySQL 연동 체크리스트:**
- [ ] ✅ Connection pooling 사용
- [ ] ✅ 트랜잭션 관리
- [ ] ✅ 자동 재연결
- [ ] ✅ 쿼리 타임아웃 설정

#### Prometheus Integration
**Prometheus 연동 체크리스트:**
- [ ] ✅ 메트릭 이름이 명확함 (snake_case)
- [ ] ✅ 적절한 메트릭 타입 사용 (Counter/Gauge/Histogram)
- [ ] ✅ 레이블이 과도하지 않음 (<10개)
- [ ] ✅ /metrics 엔드포인트 정상 노출

---

### 3.2 Configuration Management

#### Environment Variables
**환경 변수 관리:**
- [ ] ✅ 단일 설정 소스 원칙 준수 (.env.template)
- [ ] ✅ 기본값이 표준 포트만 사용 (3306, 9090)
- [ ] ✅ IP/호스트명은 기본값 없음
- [ ] ✅ 명확한 에러 메시지

**설정 검증 예시:**
```python
class MySQLConfig:
    def __init__(self):
        self.host = os.getenv('MYSQL_HOST')
        self.port = int(os.getenv('MYSQL_PORT', '3306'))
        
    def validate(self):
        if not self.host:
            raise ValueError(
                "MYSQL_HOST is not set. "
                "Set it in .env file: MYSQL_HOST=192.168.150.110"
            )
```

---

### 3.3 PySpark Best Practices

#### Spark Session Management
**Spark 세션 관리:**
- [ ] ✅ 싱글톤 패턴 사용
- [ ] ✅ 적절한 설정 (메모리, 코어)
- [ ] ✅ Graceful shutdown

#### Streaming Best Practices
**스트리밍 모범 사례:**
- [ ] ✅ 적절한 배치 간격 (5-10초)
- [ ] ✅ Checkpoint 설정
- [ ] ✅ Watermark 설정 (늦은 데이터 처리)

---

## 4. Code Review Findings

### Critical Issues (🔴 Must Fix)
1. **[이슈 제목]**
   - **파일:** `[파일명]:[라인번호]`
   - **문제:** [구체적 설명]
   - **영향:** [심각도]
   - **해결방안:** [구체적 수정 제안]

### Major Issues (🟠 Should Fix)
1. **[이슈 제목]**
   - **파일:** `[파일명]:[라인번호]`
   - **문제:** [구체적 설명]
   - **해결방안:** [구체적 수정 제안]

### Minor Issues (🟡 Nice to Fix)
1. **[이슈 제목]**
   - **파일:** `[파일명]:[라인번호]`
   - **문제:** [구체적 설명]
   - **해결방안:** [구체적 수정 제안]

### Positive Highlights (✅ Good Practices)
1. **[좋은 점 제목]**
   - **파일:** `[파일명]:[라인번호]`
   - **설명:** [왜 좋은 코드인지]

---

## 5. Recommendations

### Immediate Actions
**즉시 수정 필요:**
1. [액션 1]
2. [액션 2]

### Future Improvements
**향후 개선사항:**
1. [개선 1]
2. [개선 2]

### Best Practice Suggestions
**모범 사례 제안:**
1. [제안 1]
2. [제안 2]

---

## 6. Review Summary

### Overall Score
**종합 평가:**
- Code Structure: ⭐⭐⭐⭐⭐ (5/5)
- Readability: ⭐⭐⭐⭐☆ (4/5)
- Error Handling: ⭐⭐⭐☆☆ (3/5)
- Performance: ⭐⭐⭐⭐☆ (4/5)
- Security: ⭐⭐⭐⭐⭐ (5/5)
- Testing: ⭐⭐⭐☆☆ (3/5)

**전체 평가:** [평균 점수] / 5

### Review Status
- [ ] ✅ Approved (승인 - 머지 가능)
- [ ] 🟡 Approved with Comments (조건부 승인 - 마이너 이슈 수정 후 머지)
- [ ] 🔴 Changes Requested (변경 요청 - 크리티컬 이슈 수정 필요)

### Reviewer Notes
**리뷰어 코멘트:**
[전체적인 소감 및 추가 코멘트]

---

## 7. AI Agent Instructions

### Review Workflow
🎯 **MANDATORY PROCESS:**

1. **코드 읽기:**
   - 모든 변경된 파일을 순차적으로 읽으세요
   - 전체 맥락을 이해한 후 세부 검토 시작
   - 관련된 기존 코드도 함께 검토

2. **체계적 검토:**
   - 위 체크리스트를 하나씩 확인하세요
   - 각 카테고리별로 발견사항 기록
   - 긍정적인 부분도 함께 언급

3. **우선순위 설정:**
   - Critical > Major > Minor 순으로 분류
   - 심각도와 영향 범위 명시
   - 구체적인 해결 방안 제시

4. **건설적 피드백:**
   - 문제만 지적하지 말고 해결책 제안
   - 좋은 코드는 칭찬하여 강화
   - 학습 기회로 활용 (Best Practice 공유)

5. **실행 가능한 결과:**
   - 모호한 표현 피하기
   - 구체적인 코드 예시 제공
   - 우선순위별 액션 아이템 정리

### Communication Guidelines
- 비판적이지 않고 협력적인 톤 유지
- "이렇게 하면 안 됨" → "이렇게 하면 더 좋음"
- 명령이 아닌 제안 형태로 표현
- 항상 이유와 근거를 함께 설명

---

## 8. Checklist

### Pre-Review
- [ ] 변경된 파일 목록 확인
- [ ] 변경 이유 및 배경 파악
- [ ] 관련 이슈/작업 문서 검토

### During Review
- [ ] 코드 구조 검토 완료
- [ ] 가독성 검토 완료
- [ ] 에러 핸들링 검토 완료
- [ ] 성능 검토 완료
- [ ] 보안 검토 완료
- [ ] 테스트 검토 완료

### Post-Review
- [ ] 발견사항 정리 완료
- [ ] 우선순위 분류 완료
- [ ] 해결 방안 제시 완료
- [ ] 리뷰 상태 결정 완료

---

## 9. Notes & Observations

### Additional Comments
[추가 코멘트]

### Questions for Author
**작성자에게 질문:**
1. [질문 1]
2. [질문 2]

### Learning Points
**배운 점:**
[이번 리뷰를 통해 배운 내용이나 인사이트]










