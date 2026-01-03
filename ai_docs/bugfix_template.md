# AI Bug Fix Template - Real-Time Log Anomaly Detection System

> **프로젝트:** 실시간 로그 기반 이상 징후 감지 시스템  
> **환경:** Windows Local (Cursor IDE)  
> **템플릿 타입:** 디버깅 및 버그 수정

---

## 1. Bug Report

### Bug Title
**Title:** [예: "Kafka Consumer 연결 끊김 후 재연결 실패"]

### Severity Level
- [ ] 🔴 Critical (시스템 전체 중단)
- [ ] 🟠 High (핵심 기능 불가)
- [ ] 🟡 Medium (일부 기능 저하)
- [ ] 🟢 Low (사소한 불편)

### Discovery Context
**발견 경위:**
[버그를 어떻게 발견했는지 기술]

**재현 빈도:**
- [ ] 항상 재현됨 (100%)
- [ ] 자주 발생 (50% 이상)
- [ ] 가끔 발생 (10-50%)
- [ ] 드물게 발생 (10% 미만)

---

## 2. Bug Symptoms

### Error Messages
**에러 메시지:**
```
[실제 에러 메시지 또는 스택 트레이스 붙여넣기]
```

### Visual Symptoms
**UI/동작 증상:**
- [예: "웹 UI에 로그가 표시되지 않음"]
- [예: "Prometheus 메트릭이 업데이트되지 않음"]

### Affected Components
**영향받는 컴포넌트:**
- [ ] Kafka Consumer (`src/kafka/consumer.py`)
- [ ] PySpark Streaming (`src/spark/streaming.py`)
- [ ] MySQL Client (`src/storage/mysql_client.py`)
- [ ] ML Algorithms (`src/ml/`)
- [ ] REST API (`src/api/rest_api.py`)
- [ ] Prometheus Exporter (`src/metrics/prometheus_exporter.py`)
- [ ] Web UI (`ui/`)
- [ ] Configuration (`config/`)

---

## 3. Reproduction Steps

### Prerequisites
**재현 전 준비사항:**
1. [예: "Kafka 클러스터 실행 중"]
2. [예: "MySQL 서버 연결 가능"]
3. [예: "로그 수집 시작 상태"]

### Step-by-Step Reproduction
**재현 절차:**
1. [1단계]
2. [2단계]
3. [3단계]
4. [관찰: 예상 결과 vs 실제 결과]

### Expected vs Actual Behavior
**예상 동작:**
```
[정상적으로 작동했을 때의 동작 설명]
```

**실제 동작:**
```
[현재 발생하는 비정상 동작 설명]
```

---

## 4. Environment Details

### System Information
- **OS:** Windows 10/11
- **Python Version:** 3.9.6
- **Spark Version:** 3.4.1
- **관련 라이브러리 버전:**
  ```
  kafka-python==2.0.2
  mysql-connector-python==8.0.33
  prometheus_client==0.16.0
  scikit-learn==1.2.2
  ```

### External Services Status
- **Kafka Cluster:**
  - Status: [Running / Down / Unstable]
  - Bootstrap Servers: `192.168.150.115:9092,192.168.150.120:9092,192.168.150.125:9092`
- **MySQL Server:**
  - Status: [Running / Down / Unstable]
  - Host: `192.168.150.110:3306`
- **Prometheus:**
  - Status: [Running / Down / Unstable]
  - Endpoint: `192.168.150.110:19090`

### Recent Changes
**최근 변경사항:**
- [예: "어제 Kafka Consumer 재연결 로직 수정"]
- [예: "config 파일 업데이트"]

---

## 5. Root Cause Analysis

### Initial Hypothesis
**초기 가설:**
[버그의 원인으로 추정되는 내용]

### Code Investigation
**관련 코드 조사:**

**의심 파일 1:** `[파일 경로]`
```python
# 문제가 있을 것으로 추정되는 코드 스니펫
```

**의심 파일 2:** `[파일 경로]`
```python
# 문제가 있을 것으로 추정되는 코드 스니펫
```

### Log Analysis
**로그 분석:**
```
[관련 로그 메시지 붙여넣기]
```

### Root Cause Identification
**근본 원인:**
[최종 확인된 버그의 근본 원인]

---

## 6. Fix Strategy

### Approach
**수정 전략:**
1. [수정 방법 1: 설명]
2. [수정 방법 2: 대안]

**선택한 접근법:** [방법 X를 선택한 이유]

### Impacted Areas
**영향 범위:**
- 수정할 파일:
  - `[파일 1]`: [변경 내용]
  - `[파일 2]`: [변경 내용]

### Risk Assessment
**변경 리스크:**
- [ ] 낮음 (로컬 변경만)
- [ ] 중간 (여러 파일 수정)
- [ ] 높음 (핵심 로직 변경)

**리스크 완화 방안:**
- [예: "변경 전 기존 코드 백업"]
- [예: "단계별 테스트 진행"]

---

## 7. Implementation

### Code Changes

#### 변경 1: `[파일명]`
**변경 전:**
```python
# 기존 코드
```

**변경 후:**
```python
# 수정된 코드
```

**변경 이유:** [설명]

#### 변경 2: `[파일명]`
**변경 전:**
```python
# 기존 코드
```

**변경 후:**
```python
# 수정된 코드
```

**변경 이유:** [설명]

### Configuration Changes
**설정 변경:**
- [ ] `requirements.txt` 업데이트 필요
- [ ] `.env` 파일 변수 추가 필요
- [ ] 데이터베이스 스키마 변경 필요

---

## 8. Testing & Verification

### Test Plan
**테스트 계획:**
1. [ ] 버그 재현 테스트 (수정 전)
2. [ ] 수정 후 재현 불가 확인
3. [ ] 관련 기능 회귀 테스트
4. [ ] Edge Case 테스트

### Test Cases
**테스트 케이스 1:** [설명]
- Input: [입력]
- Expected Output: [예상 결과]
- Actual Output: [실제 결과]
- Status: [ ] Pass / [ ] Fail

**테스트 케이스 2:** [설명]
- Input: [입력]
- Expected Output: [예상 결과]
- Actual Output: [실제 결과]
- Status: [ ] Pass / [ ] Fail

### Verification Results
**검증 결과:**
- [ ] 원래 버그 해결 확인
- [ ] 새로운 버그 미발생 확인
- [ ] 성능 저하 없음 확인
- [ ] 로그 정상 출력 확인

---

## 9. Prevention & Follow-up

### Prevention Measures
**재발 방지책:**
1. [예: "에러 핸들링 추가"]
2. [예: "자동 재연결 로직 강화"]
3. [예: "모니터링 알람 추가"]

### Code Quality Improvements
**코드 품질 개선:**
- [ ] 타입 힌트 추가
- [ ] Docstring 업데이트
- [ ] 에러 메시지 개선
- [ ] 로깅 레벨 조정

### Documentation Updates
**문서 업데이트:**
- [ ] README 업데이트
- [ ] 코드 주석 추가
- [ ] 트러블슈팅 가이드 추가

### Related Issues
**관련 이슈:**
- [다른 잠재적 버그나 개선사항]

---

## 10. AI Agent Instructions

### Debugging Workflow
🎯 **MANDATORY PROCESS:**

1. **버그 분석:**
   - 에러 메시지와 스택 트레이스를 철저히 분석하세요
   - 관련 파일들을 모두 읽고 코드 흐름을 파악하세요
   - 로그 파일을 확인하여 추가 단서를 찾으세요

2. **근본 원인 파악:**
   - 증상이 아닌 근본 원인을 찾으세요
   - 재현 가능한 최소 코드를 식별하세요
   - 외부 서비스 상태도 확인하세요

3. **수정 전 검증:**
   - 버그를 재현할 수 있는지 확인하세요
   - 수정 전 테스트 케이스를 작성하세요

4. **수정 구현:**
   - 최소한의 변경으로 문제를 해결하세요
   - 새로운 버그를 만들지 않도록 주의하세요
   - 에러 핸들링과 로깅을 추가하세요

5. **검증 및 테스트:**
   - 버그가 해결되었는지 확인하세요
   - 관련 기능들이 정상 작동하는지 테스트하세요
   - Edge case를 테스트하세요

6. **문서화:**
   - 버그 원인과 해결 방법을 명확히 기록하세요
   - 재발 방지책을 문서화하세요

### Communication Guidelines
- 버그 분석 과정을 단계별로 보고하세요
- 불확실한 부분이 있으면 가설을 명시하세요
- 수정 전후 비교를 명확히 제시하세요
- 추가 테스트가 필요한 영역을 제안하세요

---

## 11. Checklist

### Pre-Fix
- [ ] 버그 재현 완료
- [ ] 근본 원인 파악 완료
- [ ] 수정 전략 수립 완료
- [ ] 백업 완료 (필요시)

### During Fix
- [ ] 코드 수정 완료
- [ ] 에러 핸들링 추가
- [ ] 로깅 추가/개선
- [ ] 주석 업데이트

### Post-Fix
- [ ] 버그 재현 불가 확인
- [ ] 관련 기능 테스트 완료
- [ ] 회귀 테스트 완료
- [ ] 문서 업데이트 완료

### Documentation
- [ ] 이 템플릿 작성 완료
- [ ] 코드 주석 추가 완료
- [ ] README 업데이트 (필요시)

---

## 12. Notes & Observations

### Additional Notes
[추가로 기록할 내용]

### Lessons Learned
[이번 버그를 통해 배운 점]

### Future Improvements
[향후 개선 아이디어]










