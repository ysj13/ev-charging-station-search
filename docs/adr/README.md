# 설계 결정 기록 (ADR)

서비스의 주요 설계 결정과 그 이유를 기록합니다. 각 문서는 맥락, 결정, 검토한 대안, 결과(얻은 것과 감수한 것)로 구성됩니다.

| 번호 | 제목 | 상태 |
|---|---|---|
| [001](001-status-collection-and-snapshot-catch-up.md) | 상태 수집: 짧은 주기 폴링과 전체 동기화 SNAPSHOT 따라잡기 | 승인 |
| [002](002-sync-cleanup-safeguards.md) | 전체 동기화의 데이터 정리 안전장치 | 승인 |
| [003](003-vacancy-definition.md) | 빈자리 정의: 충전중 → 충전대기 | 승인 |
| [004](004-detector-ordering-and-delivery.md) | 빈자리 감지의 처리 순서와 전달 보장 | 승인 |
| [005](005-single-writer-for-charger-status.md) | 충전기 상태를 쓰는 주체를 감지기 하나로 | 승인 |
| [006](006-station-search-query.md) | 충전소 검색 쿼리 설계 | 승인 |
| [007](007-public-api-resilience.md) | 공공 API 호출 안정성 | 승인 |
| [008](008-kafka-and-rabbitmq-roles.md) | Kafka와 RabbitMQ의 역할 분리 | 승인 |
| [009](009-realtime-screen-updates.md) | 검색 화면의 실시간 갱신: 빈자리는 SSE, 나머지는 주기 조회 | 승인 |
| [010](010-vacancy-notification-delivery.md) | 빈자리 알림 발송: 매칭 → RabbitMQ → Redis Pub/Sub → SSE | 승인 |
