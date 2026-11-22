# 시연 가이드

실제 공공 데이터가 바뀌기를 기다리지 않고 "충전이 끝나 빈자리가 생기는" 장면을 검색 화면에서 재현하는 방법입니다.

## 준비

1. `.env`의 `DEV_TOOLS_ENABLED`를 `true`로 바꿉니다. 인증키(`DATA_GO_KR_SERVICE_KEY`)도 들어 있어야 실데이터로 시연할 수 있습니다.
2. 지도를 함께 보려면 `KAKAO_MAP_JS_KEY`를 넣고 카카오 개발자 콘솔 설정을 마칩니다(README의 실행 방법 참고). 지도 없이도 목록과 실시간 알림은 동작합니다.
3. 애플리케이션을 실행합니다.

   ```powershell
   ./gradlew bootRun
   ```

4. 로그에 `기동 시 충전소 전체 동기화 완료`가 나올 때까지 기다립니다. 앱 기동 뒤 동기화에 서울 기준 약 18초가 걸립니다.

## 시연 순서

브라우저에서 http://localhost:8280 을 엽니다.

| 단계 | 할 일 | 보이는 것 |
|---|---|---|
| 1 | 화면 오른쪽 위 표시 확인 | "실시간" (빈자리 스트림 연결됨) |
| 2 | 지도를 움직이거나 `강남구청`을 검색 | 충전소 목록과 지도 핀. 핀 색은 빈자리 있음(초록), 모두 사용 중(빨강), 사용 불가(회색) |
| 3 | 충전소를 누름 | 충전기별 상태와 상태 갱신 시각 |
| 4 | 아래 명령으로 그 충전소의 충전기를 **충전중**으로 바꿈 | 2분 안에(또는 다시 검색하면) 상태가 충전중으로 바뀜 |
| 5 | 같은 충전기를 **충전대기**로 바꿈 | 1~2초 안에 "○○ 01번 충전기가 비었어요 (시연)" 알림, 핀이 깜박이고 상세가 바로 갱신됨 |

충전소 ID는 상세 화면을 연 상태에서 `/api/stations?q=검색어` 응답의 `stationId`로 확인할 수 있습니다.

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8280/dev/chargers/KP002507/01/status -ContentType 'application/json' -Body '{"status":"CHARGING"}'
```

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8280/dev/chargers/KP002507/01/status -ContentType 'application/json' -Body '{"status":"AVAILABLE"}'
```

```powershell
Invoke-RestMethod -Uri http://localhost:8280/dev/vacancies
```

### 빈자리 알림

| 단계 | 할 일 | 보이는 것 |
|---|---|---|
| 1 | 충전소 상세에서 **빈자리 알림 받기**를 누름 | 버튼이 "알림 받는 중 · 끄기"로 바뀜. 처음이면 브라우저 알림 권한을 물음 |
| 2 | 상세의 **테스트 빈자리 만들기 (개발용)** 를 누름 (또는 위의 두 명령으로 충전중 → 충전대기로 바꿈) | "🔔 ○○ 01번 충전기에 자리가 났어요" 알림. 옆으로 밀거나 ×를 누를 때까지 남는다. 알림을 누르면 그 충전소 상세가 열리고 빈자리가 난 충전기가 강조된다. 다른 탭을 보고 있으면 브라우저 알림 |
| 3 | 상세를 닫고 위쪽 **내 알림**을 누름 | 알림 받는 충전소와 최근 알림 |

테스트 버튼은 그 충전소의 첫 충전기를 충전중으로 바꾼 뒤 바로 충전대기로 바꿉니다. 아래 상태 주입과 같아서, 그 충전기는 원본 상태가 다시 바뀔 때까지 충전대기로 남습니다(실제 상태를 덮어씀). 실제 수집과 같은 경로(상태 → 빈자리 감지 → DB → 알림)를 그대로 탑니다. 버튼은 `DEV_TOOLS_ENABLED=true`일 때만 보입니다.

실제 알림은 같은 충전소에 10분에 한 번만 가지만, 시연용 상태(`MANUAL`)로 만든 알림은 10분 제한 없이 누를 때마다 갑니다. 시연 알림은 실제 알림의 10분 계산에도 영향을 주지 않습니다.

충전중으로 바꾸는 것은 빈자리 이벤트가 아니라서 화면에 즉시 밀려오지 않습니다. 화면은 이런 변화를 2분마다 다시 읽어 맞춥니다([ADR-009](adr/009-realtime-screen-updates.md)).

## 흐름을 눈으로 확인하기

| 도구 | 주소 | 볼 것 |
|---|---|---|
| Kafka UI | http://localhost:8290 | `charger.status`에 `source: MANUAL` 메시지, `charger.available`에 빈자리 이벤트 |
| 지표 | http://localhost:8280/actuator/metrics/detector.vacancy.events | 발행한 빈자리 이벤트 수 |
| 지표 | http://localhost:8280/actuator/metrics/vacancy.stream.connections | 연결된 화면 수 |
| RabbitMQ 관리 콘솔 | http://localhost:15672 | `notification.send.queue` 처리량, 실패한 작업이 쌓이는 `notification.send.dlq` |
| 지표 | http://localhost:8280/actuator/metrics/notification.sent | 보낸 알림 수 (`notification.matched`, `notification.failed`도 있음) |

## 참고

- 주입한 상태는 실제 수집과 같은 경로(Kafka `charger.status`)로 들어가 빈자리 감지 → DB → 검색 API → 화면 알림까지 그대로 통과합니다.
- 주입한 상태의 갱신 시각은 현재 시각 이후로 정해집니다. 그래서 그 충전기는 원본에서 상태가 다시 바뀔 때까지 주입한 상태로 남습니다. 재기동이나 전체 동기화로는 되돌아가지 않습니다(더 오래된 상태는 반영하지 않는 규칙). 시연에는 실제 운영 데이터가 아닌 로컬 데이터만 쓰세요.
- 운영 환경에서는 `DEV_TOOLS_ENABLED`를 켜지 않습니다. 인증이 없는 쓰기 기능입니다.
