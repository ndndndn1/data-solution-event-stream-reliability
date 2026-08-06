# data-solution-event-stream-reliability

<!-- engineering-completeness:start -->
## Engineering completeness

- Score: **81.85/100** (44.20/54)
- Technical maturity: **PORTFOLIO_READY**
- Reachable stub ratio: **0.00%**
- Requirement-weighted stub ratio: **0.00%**
- Critical feature stub: **absent**
- Verified feature scope: composite failure and recovery scenario, deterministic synthetic event stream, idempotent deduplication, retry, DLQ, and replay recovery
- Reproducible clean run: **yes**
- Rubric: `1.0.0` · Commit: `6446d8979f69`
<!-- engineering-completeness:end -->

광고·이벤트 데이터 파이프라인에서 대량 이벤트의 중복·지연·실패를 다루고,
장애 이후 안전하게 재처리할 수 있는지를 보여 주는 실행 가능한 Python
프로토타입입니다.

## 공고 핵심 요구사항과 구현 방향

기존 공고가 강조한 데이터 신뢰성을 아래처럼 작은 소비자 파이프라인으로
구체화했습니다.

| 핵심 요구사항 | 구현 방향 | 확인 지표 |
| --- | --- | --- |
| 대량 이벤트의 정합성과 중복 방지 | 이벤트 `id`를 idempotency key로 사용합니다. 한 배치 안의 중복뿐 아니라 같은 파이프라인 인스턴스에 다시 전달된 이벤트도 한 번만 처리합니다. 성공한 이벤트만 처리 완료 상태에 기록합니다. | `duplicates` |
| 잘못된 데이터 격리 | `id`, `payload`, 선택적 `event_time`을 검증하고, 실패한 원본과 사유를 DLQ 항목으로 보존합니다. | `validation_failures` |
| 일시적 장애에 대한 복원력 | downstream 쓰기를 `processor`로 분리하고, 예외 발생 시 설정된 횟수만큼 즉시 재시도합니다. 모두 실패하면 시도 횟수와 마지막 오류를 DLQ로 보냅니다. | `retry_attempts`, `processing_failures` |
| 지연 도착 이벤트 관측 | timezone이 포함된 event time과 현재 시각을 비교합니다. 허용 지연을 넘겨도 데이터 손실 없이 처리하되 별도 집계합니다. | `late_records` |
| 장애 데이터 재처리와 복구 확인 | 이전 실행의 DLQ를 `replay()`에 그대로 전달할 수 있습니다. 실패 전에 성공 처리된 이벤트는 중복 방지 상태에 포함되지 않으므로 복구 후 다시 처리됩니다. | `replayed`, `recovered` |
| 운영 가시성 | 각 실행의 `PipelineResult.metrics`와 인스턴스 누적 `pipeline.metrics`를 함께 제공합니다. | `received`, `accepted` 및 위 지표 |

## 동작 예시

```python
from data_solution_event_stream_reliability import ReliablePipeline

available = False

def write_to_sink(record):
    if not available:
        raise ConnectionError("sink unavailable")

pipeline = ReliablePipeline(write_to_sink, max_retries=2)
failed = pipeline.process([
    {"id": "event-1", "payload": {"campaign_id": 42}},
])

# 장애 복구 뒤 DLQ 재처리
available = True
replayed = pipeline.replay(failed.dead_letter)
assert replayed.metrics.recovered == 1
```

이벤트 형식은 다음과 같습니다.

```python
{
    "id": "고유한 문자열",              # 필수
    "payload": {"key": "value"},      # 필수, None 불가
    "event_time": "2026-08-06T09:00:00+09:00",  # 선택, timezone 필수
}
```

`max_retries=2`는 최초 시도 뒤 최대 두 번 더 시도한다는 뜻입니다.
`max_lateness=None`으로 지연 판정을 끌 수 있습니다. 기본 허용 지연은 5분입니다.

## 실행

Python 3.12 이상에서 외부 런타임 의존성 없이 실행됩니다.

```bash
python -m pip install -e .
python -m unittest discover -s tests -v
```

## 대량 이벤트 복합 장애 시나리오

광고 노출 이벤트가 한꺼번에 유입되는 동안 중복 전송, 늦은 도착, 잘못된
스키마, downstream의 일시적 timeout과 지속 장애가 함께 발생하는 상황을
재현합니다. 기본 시나리오는 원본 100,000건을 다음과 같이 구성합니다.

| 주입 조건 | 비율 | 기본 건수 | 처리 방식 |
| --- | ---: | ---: | --- |
| 중복 전달 | 5% | 5,000 | 같은 `id`를 한 번만 처리 |
| 스키마 오류 | 1% | 1,000 | 수정 전까지 DLQ에 격리 |
| 지연 도착 | 10% | 10,000 | 처리하되 `late_records`로 관측 |
| 일시적 timeout | 2% | 2,000 | 첫 시도 실패 후 retry에서 복구 |
| 지속 장애 | 0.5% | 500 | retry 소진 후 DLQ, 장애 복구 뒤 replay |

고정 seed를 사용하므로 선택되는 이벤트와 전달 순서까지 재현됩니다. 기본
시나리오는 총 105,000건을 전달하여 초기 98,500건을 처리하고, 지속 장애
500건을 replay로 회수합니다. 최종적으로 유효한 고유 이벤트 99,000건은
정확히 한 번 처리되고 스키마 오류 1,000건만 DLQ에 남아야 합니다.

```bash
event-stream-scenario
event-stream-scenario --events 10000 --seed 7
event-stream-scenario --json
```

기본 출력은 사람이 읽는 요약이며 `--json`은 자동화에서 사용할 수 있는
구조화된 보고서를 출력합니다. 종료 코드는 모든 정합성 검사가 통과하면
`0`, 실패하면 `1`입니다.

### 데이터 생성기 재사용

대량 데이터 생산 로직은 CLI와 분리되어 있습니다. 생성 결과는 여러 번
순회해도 같은 레코드 순서를 만들며, 생성기 자체는 특정 consumer에
의존하지 않습니다.

```python
from data_solution_event_stream_reliability import (
    EventGenerationConfig,
    EventStreamGenerator,
)

config = EventGenerationConfig(event_count=1_000_000, seed=42)
stream = EventStreamGenerator(config).generate()

for record in stream:
    # Kafka producer, 파일 writer 또는 별도 pipeline에 전달
    produce(record)

print(stream.summary)
print(stream.failure_plan)
```

`FaultInjectingProcessor`도 실제 downstream callable을 감싸도록 분리되어
있어, 생성기와 별개로 재시도·장애 복구 테스트에 사용할 수 있습니다.
이 시나리오는 기능적 정합성 검증이며 실행 시간이나 초당 처리량을 운영
벤치마크 결과로 간주하지 않습니다.

## 범위와 확장 방향

이 저장소는 공고의 신뢰성 문제를 코드로 설명하기 위한 단일 프로세스
프로토타입이며 처리량 벤치마크나 운영 완료를 주장하지 않습니다. 현재 중복
상태와 지표는 메모리에 있고 재시도에는 실제 대기 시간이 없습니다. 운영 환경에서는
각 경계를 다음과 같이 교체할 수 있습니다.

- 입력/재처리: Kafka 같은 영속 이벤트 로그와 consumer offset
- idempotency 상태: Redis 또는 트랜잭션 DB의 unique key
- 재시도/DLQ: 지수 backoff, retry topic, 보존 기간 및 재처리 정책
- 관측성: Prometheus/OpenTelemetry 지표와 지연·DLQ 알림
- 확장성 검증: 파티션별 순서, consumer scale-out, 장애 주입 및 부하 테스트
