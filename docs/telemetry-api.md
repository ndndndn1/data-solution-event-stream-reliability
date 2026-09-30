# 공장 텔레메트리: 영속 수집 REST API

## 해결한 경계

기존 `ReliablePipeline`은 동기식·메모리 기반 이벤트 시뮬레이터입니다.
동작을 바꾸지 않고 별도 `TelemetryStore`와 FastAPI 모듈을 추가했습니다.
프로세스 재시작 뒤에도 중복 상태와 측정값이 남으며, 데이터 저장과 중복 판정은
같은 SQLite 트랜잭션 안에서 수행됩니다.

원프레딕트 Backend 공고 <https://www.wanted.co.kr/wd/386088>에서 강조하는
Python/FastAPI, HTTP REST, SQL/RDB, async/await, 테스트, 공장 시계열을
작은 실행 가능한 결과물로 연결합니다. 지원자의 경력 연수나 해당 기업의 실제
환경을 증명하지 않으며, Kafka/RabbitMQ 운영과 배치 ETL 경험을 구현했다고
주장하지 않습니다. 예제는 전부 가상 공장·장비·센서 데이터입니다.

## 설치와 검증

Python 3.12 이상. 기본 라이브러리와 기존 CLI는 여전히 외부 런타임 의존성이
없고, API와 API 테스트는 선택 의존성을 설치해야 합니다.

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate
python -m pip install -e '.[api,test]'
python -m unittest discover -s tests -v
python examples/http_demo.py
```

마지막 명령은 임시 DB와 127.0.0.1의 임시 포트를 사용합니다. 실제 Uvicorn
프로세스를 띄워 수집한 뒤 종료/재시작하고, 재전송 중복 판정·409 충돌·시간순
조회·평균 25를 검증합니다. 성공 시 PASS JSON, 실패 시 non-zero로 종료합니다.
외부 서비스나 실제 공장 데이터는 필요하지 않습니다.

수동 실행:

```bash
# DB 경로의 상위 디렉터리는 미리 존재해야 합니다.
export TELEMETRY_DB="$PWD/telemetry.sqlite3"
python -m uvicorn data_solution_event_stream_reliability.api:app --host 127.0.0.1 --port 8000
curl http://127.0.0.1:8000/readyz
curl -H 'Content-Type: application/json' --data-binary @examples/telemetry-batch.json http://127.0.0.1:8000/v1/telemetry
curl 'http://127.0.0.1:8000/v1/factories/factory-demo/equipment/pump-01/telemetry?metric=temperature&limit=100'
```

첫 POST는 `{"inserted":2,"duplicates":0}`, 동일 재전송은
`{"inserted":0,"duplicates":2}`입니다. 서버를 재시작해도 같은 DB를 지정하면
중복 상태가 유지됩니다. 파일 삭제는 상태 초기화이므로 주의하세요.

## API 계약

- `GET /readyz`: 현재 DB 연결 및 telemetry 테이블 읽기가 되면 200, 실패 시 503
  (쓰기 가능성·디스크 여유·외부 의존성을 검증하는 운영 readiness는 아님)
- `POST /v1/telemetry`: JSON 배열, 1–100개 이벤트, 요청 본문 최대 128 KiB
  (스트리밍 본문도 누적 크기를 확인). Content-Type은 application/json
- `GET /v1/factories/{factory_id}/equipment/{equipment_id}/telemetry?metric=temperature&limit=100`:
  해당 공장/장비/metric의 가장 오래된 이벤트부터 조회. limit은 1–1000
- 이벤트 필수 필드: factory_id, equipment_id, event_id, observed_at, metric, value, unit
  알 수 없는 필드와 중복 JSON 키는 거부
- ID: 1–64자 ASCII 영숫자로 시작, 이후 영숫자·점·밑줄·하이픈 허용
- observed_at: ISO 8601 부분집합 YYYY-MM-DDTHH:MM:SS[.ffffff]Z 또는 ±HH:MM
  offset. 소수점은 최대 6자리이며 과도한 정밀도는 잘라내지 않고 거부. UTC/마이크로초 형식으로 정규화
- metric/unit: temperature/degC, vibration/mm/s, pressure/kPa만 허용
- value: bool/string이 아닌 유한 숫자, 범위 ±1e100. 제한된 집계에서도 오버플로를
  피하기 위한 표현 범위이며 실제 센서의 물리적 유효 범위 검증은 별도 과제.
  저장/비교 숫자는 binary64이며 정수를 float로 변환할 때 값이 바뀌면 422로 거부.
  JSON 소수는 일반 Python JSON의 binary64 정밀도로 해석되므로 정밀 십진 계측에는
  적합하지 않음
- 성공 200: inserted와 duplicates 카운트. 완전히 같은 배치 재전송도 200
- 같은 `(factory_id, event_id)`의 정규화된 payload hash가 같으면 중복,
  다르면 409. 장비/시간/측정값/단위 변경도 충돌이며 기존 값을 덮어쓰지 않음
- 409 또는 저장 실패 시 배치 전체 rollback. 잘못된 이벤트는 저장 전에 전체 검증
- 422: 스키마/JSON/쿼리 오류, 413: 본문 크기 초과, 415: Content-Type 오류
- 503 + Retry-After: 1: DB 오류. 원문 SQL 오류/로컬 경로는 응답에 노출하지 않음
  잠금 timeout은 5초. 클라이언트는 같은 ID와 payload로 제한적 backoff 재시도 가능

조회 결과는 `(observed_at, event_id)` 순서로 결정됩니다. 늦게 도착한 이벤트를
버리거나 현재 시각으로 덮어쓰지 않습니다. aggregate의 count/min/max/mean은
응답에 실린 이벤트만 대상으로 하며 unit과 scope=returned_events를 명시합니다.
limit을 넘는 데이터가 있으면 has_more=true입니다. 전체 기간 집계나 페이지
순회 기능은 제공하지 않습니다.

## 설계와 검증 근거

SQLite PRIMARY KEY(factory_id, event_id), SHA-256 payload hash,
BEGIN IMMEDIATE, WAL, synchronous=FULL을 사용합니다. 정규화로 UTC timezone
표현이나 23/23.0 같은 동등한 표현은 재전송으로 처리합니다. 각각의 DB 작업은
자체 연결을 열고 닫습니다. 쓰기 잠금 획득 후 키를 조회하여 경쟁 요청도 한 건만
삽입합니다. FastAPI async 핸들러는 동기 DB 초기화·저장·조회·readiness를
thread pool로 넘기므로 SQLite 호출이 이벤트 루프를 직접 막지 않습니다.

테스트는 파일 DB 재개방, 8개 연결의 동시 중복 전송, 충돌 시 배치 rollback,
서로 다른 공장의 동일 ID, commit 직전 예외 후 재시도, 지연/역순 데이터,
입력 검증, 제한된 집계, API 오류 계약, 실제 HTTP 서버 재시작을 포함합니다.
실제 SQLite 쓰기 잠금 timeout 후 재시도, commit 직전 자식 프로세스 강제 종료도
검증합니다.
기존 시뮬레이터 회귀 테스트도 함께 실행합니다.

## 명시적 한계와 다음 단계

- 단일 호스트 학습/포트폴리오 프로토타입입니다. Kafka/RabbitMQ, PostgreSQL,
  broker ack/offset, 분산 exactly-once, 배치 ETL, 고가용성 구현이 아닙니다
- 기존 ReliablePipeline과 별도 경로이며 기존 DLQ/retry/late counter가 API에
  자동 연결되지 않습니다. 이 API는 원자적 배치 거부 방식입니다. 외부 시스템에
  대한 exactly-once 부수 효과를 보장하지 않으며, DB 저장과 외부 전송을 묶으려면
  별도의 outbox/수신측 멱등성 설계가 필요합니다
- 인증/인가/tenant 접근 제어/TLS/rate limit/관측성/백업/복구/보존 기간/스키마
  마이그레이션은 미구현입니다. loopback에만 바인딩하고 공개 배포하지 마세요
- 공장 ID는 저장 키 격리일 뿐 보안 경계가 아닙니다. 접근 권한은 검증하지 않습니다
- SQLite 파일은 로컬 디스크용입니다. 네트워크 파일시스템/다중 호스트 운영은
  지원 범위가 아닙니다. 빈 경로와 :memory: DB는 거부합니다. thread pool 포화와 장기 쓰기 경합에 대한 부하 검증도 없음
- 프로세스 재시작·트랜잭션 예외 검증은 전원 장애/디스크 손상 내구성의 증거가 아님
- FastAPI 자동 문서의 POST body 스키마는 수동 Request 처리 때문에 자동 생성되지
  않습니다. 이 문서와 sample JSON이 수집 계약의 기준입니다
- 다음 확장은 승인된 별도 범위로 broker 소비/ack와 트랜잭션 경계, PostgreSQL
  전환, 명시적 시간 구간/커서 페이지, 인증과 실제 부하 측정을 구현할 수 있습니다
