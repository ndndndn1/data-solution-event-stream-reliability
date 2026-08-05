# data-solution-event-stream-reliability

Domain: 광고·이벤트 데이터 파이프라인

Purpose: 대량 이벤트의 중복·지연·실패를 다루고 재처리 가능성을 검증합니다.

This repository is an executable scaffold. It is not a completed benchmark result.

## Included capabilities

- deduplication
- retry and DLQ
- replay metrics

## Run

```bash
python -m unittest discover -s tests -v
```
