# 초기 목적 기반 사용성 검증

## 프로젝트의 초기 목적

기준 README(d13428e)는 광고·이벤트 데이터를 이용한 **단일 프로세스 신뢰성
프로토타입**을 약속합니다. 중복 억제, 잘못된 원본의 DLQ 보존, 일시 실패 재시도,
지연 데이터 무손실 처리, 장애 복구 후 replay, 고정 seed 재현성이 핵심입니다.
Kafka·영속 consumer offset·분산 운영은 README의 확장 방향이었습니다.
새 FastAPI/공장 시계열 경로는 채용 공고에 맞춘 추가 범위입니다.

## 재현 가능한 검증

```bash
python -m pip install -e '.[api,test]' -c docs/tested-api-constraints.txt
python -m unittest discover -s tests -v
python -m data_solution_event_stream_reliability.scenario --json
python examples/reliability_sqlite_demo.py
python examples/http_demo.py
python -m data_solution_event_stream_reliability.benchmark --output /tmp/reliability-benchmark.json
```

기본 시나리오는 source 100,000건에서 delivered 105,000건, 초기 성공 98,500건,
중복 5,000건, replay 복구 500건, 최종 유효 99,000건, schema DLQ 1,000건을
검증합니다. 기본 benchmark는 warmup 1회와 100,000건 실행 10회입니다.
이는 합성 데이터·제어된 fault 모델 결과이며 운영 부하/HTTP SLA가 아닙니다.

`reliability_sqlite_demo.py`는 2,000개 합성 원본을 실제 로컬 SQLite sink로 보냅니다.
DB row/전체 payload를 독립적으로 만든 원본 사전과 대조하고, DLQ를 JSON 파일로
저장·재로드한 후 10개 outage 레코드를 회수합니다. 실제 DB를 닫고 다시 열어
1,980개 유효 레코드가 남는지 확인합니다. 새 pipeline으로 전체 원본을 재전송해도
sink의 unique key가 추가 row를 막습니다. 이 데모는 외부 broker 없이도 연결 가능한
실제 sink adapter의 작은 예입니다. 제품용 connector로 제공하는 것은 아닙니다.

| 초기/추가 목적 | 실제 검증 | 의미와 한계 |
| --- | --- | --- |
| 결정적 생성·대량 정합성 | 기본 100k 시나리오, 고정 seed, 7개 보존 조건 | 합성 source이며 실제 broker 없음 |
| 중복 억제·지연 무손실 | scenario 및 실제 SQLite row/payload 비교 | legacy pipeline ID 상태는 프로세스 메모리 |
| 원본 DLQ·안전한 재시도 | nested mutation 회귀, disk DLQ 재로드 | payload는 deepcopy 가능한 Python 값이어야 함 |
| 장애 복구/replay | 실제 sink 1970 + 10 = 1980; invalid 20 격리 | fault는 명시적으로 주입 |
| commit 후 응답 손실 | 실제 DB commit 직후 TimeoutError; unique sink 재시도 | 외부 sink 멱등성이 필요한 이유를 함께 검증 |
| 신규 영속 API | 실제 HTTP 종료/재시작, 동시 연결, 409, rollback, 잠금 | SQLite 내부 트랜잭션 범위 |
| 성능 관측 | 반복 whole-scenario benchmark | p95/p99는 요청 지연이 아님 |

## 발견하여 수정한 실제 결함

기존 shallow copy는 callback이 nested payload를 바꾼 뒤 실패할 때 원본 입력,
다음 재시도, DLQ 증거까지 함께 바꾸었습니다. intake, 각 processor 호출,
accepted/DLQ 출력에 독립적인 deep snapshot을 적용하고 회귀 테스트로 고정했습니다.
또한 빈 list를 상속한 callable sink처럼 bool값이 False인 유효 callback이
no-op 기본값으로 바뀌어 실제 전송 없이 accepted로 기록되던 문제를 수정했습니다.
processor/downstream/clock은 None일 때만 기본값을 사용합니다.
단순 stub 수치가 아니라 README의 '실패한 원본 보존' 요구에 대한 결함 수정입니다.

## 효과가 이미 commit된 뒤 응답이 사라지는 경계

`ReliablePipeline`의 성공 카운터는 callback의 정상 반환을 뜻합니다. callback이
외부 DB commit 이후 TimeoutError를 던지면 pipeline은 이미 반영됐는지 알 수
없습니다. 따라서 pipeline의 메모리 dedup만으로 외부 효과 exactly-once를 보장할
수 없습니다. 데모의 **비멱등 실제 SQLite negative control**은 accepted=1인데
실제 effect row가 2개 생기는 것을 확인합니다. 반대로 unique key와 동일 payload
확인을 사용하는 실제 sink에서는 같은 fault 뒤에도 event row가 한 개 유지됩니다.
운영에는 sink idempotency 또는 outbox/트랜잭션 경계·broker ack 설계가 필요합니다.

## stub 및 실제 통합 구분

- `ReliablePipeline()` 및 `FaultInjectingProcessor(..., downstream=None)`의 기본
  callback은 no-op입니다. 시뮬레이션용 기본값이고 외부 저장 완료의 증거는 아닙니다
- 기존 scenario의 successful_deliveries는 in-memory 관측입니다. SQLite 데모가
  별도로 실제 commit·재개방·payload 비교를 검증합니다
- `NotImplemented`는 metrics 덧셈의 Python 타입 프로토콜 반환값이며 기능 stub이 아님
- 테스트의 mock은 DB 오류·thread offload 경계 확인에만 쓰입니다. 실제 SQLite
  잠금/rollback/재시작과 실제 Uvicorn HTTP 데모를 대체하지 않습니다
- Kafka/RabbitMQ·ETL·인증/인가·고가용성·실제 설비 연동은 구현하지 않았습니다.
  초기 prototype 목적에서 운영 확장으로 명시된 항목과 새 공고의 추가 요구를
  구분해야 하며 이 프로젝트가 공고 전체나 경력 요건을 충족한다고 주장하지 않습니다
