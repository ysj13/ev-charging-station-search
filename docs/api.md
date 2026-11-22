# API

## 검색 API

| 메서드 | 경로 | 설명 |
|---|---|---|
| GET | `/api/stations` | 충전소 검색 |
| GET | `/api/stations/{stationId}` | 충전소 상세와 충전기별 현재 상태 |

### `GET /api/stations`

`q`, `zscode`, `lat`+`lng` 중 하나는 반드시 지정합니다. 여러 조건을 함께 주면 모두 만족하는 충전소만 돌려줍니다.

| 파라미터 | 설명 | 제약 |
|---|---|---|
| `q` | 이름·주소 부분 일치 | 공백 제외 2~50자 |
| `zscode` | 시군구 코드 (예: `11680` 강남구) | 숫자 5자리 |
| `lat`, `lng` | 기준 좌표. 결과를 가까운 순으로 정렬하고 거리(m)를 함께 돌려줌 | 함께 지정, 위도 33~39, 경도 124~132 |
| `radius` | 반경(m). 좌표와 함께만 지정 | 100~10,000, 기본 1,000 |
| `availableOnly` | 사용 가능한 충전기가 있는 곳만 | 기본 `false` |
| `minOutputKw` | 이 출력 이상인 충전기만 판단·집계 (예: `50` → 급속 위주) | 1~400 |
| `page`, `size` | 페이지 | `page` 0~1,000, `size` 1~100(기본 20), 결과의 앞 10,000건까지 |

- 정렬: 좌표 검색이면 거리순, 그 외에는 이름순입니다. 실시간으로 바뀌는 빈자리 수로는 정렬하지 않으므로, 정렬 순서가 바뀌어 생기는 중복·누락은 없습니다. 단 `availableOnly=true`이면 페이지를 넘기는 사이 상태가 바뀐 충전소가 결과에 들어오거나 빠질 수 있습니다.
- `availableOnly`와 `minOutputKw`를 함께 주면 "해당 출력 이상인 충전기 중 사용 가능한 것이 있는 곳"입니다.

```
GET /api/stations?lat=37.4979&lng=127.0276&radius=1000&availableOnly=true&minOutputKw=50
```

```json
{
  "items": [
    {
      "stationId": "ME174027",
      "name": "...",
      "address": "...",
      "latitude": 37.49,
      "longitude": 127.02,
      "distanceMeters": 211,
      "operatorName": "...",
      "useTime": "24시간 이용가능",
      "parkingFree": true,
      "chargerCount": 1,
      "availableCount": 1,
      "chargingCount": 0,
      "maxOutputKw": 100,
      "statusUpdatedAt": "2026-11-21T16:51:08Z"
    }
  ],
  "page": 0,
  "size": 20,
  "totalCount": 25
}
```

| 응답 필드 | 설명 |
|---|---|
| `distanceMeters` | 좌표 검색일 때만 |
| `chargerCount`, `availableCount`, `chargingCount`, `maxOutputKw`, `statusUpdatedAt` | `minOutputKw`를 주면 그 출력 이상인 충전기만 대상으로 계산 |
| `statusUpdatedAt` | 대상 충전기 중 가장 최근 상태 갱신 시각 |

### `GET /api/stations/{stationId}`

```json
{
  "stationId": "KP002507",
  "name": "...",
  "address": "...",
  "locationDetail": null,
  "latitude": 37.51,
  "longitude": 127.04,
  "operatorName": "...",
  "operatorCall": "...",
  "useTime": "...",
  "parkingFree": true,
  "limited": false,
  "limitDetail": null,
  "availableCount": 1,
  "chargers": [
    {
      "chargerId": "01",
      "chargerType": "06",
      "outputKw": 50,
      "status": "AVAILABLE",
      "statusDescription": "충전대기",
      "statusUpdatedAt": "2026-11-21T12:20:20Z"
    }
  ]
}
```

- `chargerType`은 공공데이터의 충전기 타입 코드 원문입니다.
- 비활성(원본에서 사라진) 충전소는 404입니다.

### 충전기 상태 값

| `status` | `statusDescription` | 공공데이터 코드 |
|---|---|---|
| `COMMUNICATION_ERROR` | 통신이상 | 1 |
| `AVAILABLE` | 충전대기 | 2 |
| `CHARGING` | 충전중 | 3 |
| `SUSPENDED` | 운영중지 | 4 |
| `MAINTENANCE` | 점검중 | 5 |
| `UNKNOWN` | 상태미확인 | 9 (그 외 값 포함) |

### 오류 응답

RFC 9457 `ProblemDetail` 형식입니다.

```json
{
  "title": "잘못된 요청 파라미터",
  "status": 400,
  "detail": "요청 파라미터가 올바르지 않습니다",
  "instance": "/api/stations",
  "errors": { "size": "페이지 크기는 100 이하여야 합니다" }
}
```

| 상태 | 경우 |
|---|---|
| 400 | 파라미터 형식·범위 오류: 제목 "잘못된 요청 파라미터", `errors`에 필드별 사유 |
| 400 | 조건 조합 오류: 제목 "잘못된 검색 조건", `errors` 없음. 검색 조건 없음, 좌표 한쪽만 지정, 좌표 없이 `radius`만 지정, 공백 제외 2자 미만 검색어, 조회 상한 초과 |
| 404 | 없거나 비활성인 충전소 |

## 실시간 스트림

### `GET /api/vacancies/stream`

빈자리 이벤트를 Server-Sent Events로 보냅니다. 브라우저에서는 `EventSource`로 연결합니다.

| 이벤트 | 시점 | data |
|---|---|---|
| `connected` | 연결 직후 1회 | `{"connectedAt": "..."}` |
| `vacancy` | 빈자리 발생 (`charger.available`) | `{"eventId", "stationId", "chargerId", "occurredAt", "source"}` |
| `notification` | 구독한 충전소의 빈자리 알림 (`clientKey`를 줬을 때만) | `{"notificationId", "eventId", "stationId", "stationName", "chargerId", "occurredAt", "sentAt"}` |

- `?clientKey=...`를 붙이면 그 구독자의 알림도 같은 연결로 받습니다. `EventSource`는 헤더를 붙일 수 없어 쿼리 파라미터로 받습니다. 형식이 틀리면 400입니다. 쿼리 문자열은 접근 로그에 남을 수 있으므로 운영에서는 이 값을 로그에서 가려야 합니다.
- 알림은 최소 한 번 전달되므로 같은 `notificationId`가 다시 올 수 있습니다.

- 25초마다 주석(`: keep-alive`)을 보내 연결을 유지합니다. 연결은 최대 30분 유지됩니다. 네트워크가 끊기면 `EventSource`가 스스로 다시 연결하지만, 503 같은 HTTP 오류를 받으면 다시 연결하지 않으므로 클라이언트가 직접 다시 시도해야 합니다(검색 화면은 30~40초 뒤 재시도).
- 연결한 뒤에 발생한 빈자리만 보냅니다. 현재 상태는 검색 API로 읽습니다.
- 모든 지역의 빈자리를 보내므로, 화면에 있는 충전소만 골라 쓰는 것은 클라이언트가 합니다.
- 인스턴스당 동시 연결이 500개를 넘거나 앱이 종료 중이면 503과 `Retry-After: 30`으로 거절합니다(`app.web.max-stream-connections`).
- 빈자리 이벤트는 최소 한 번 전달되므로 같은 `eventId`가 다시 올 수 있습니다. 클라이언트는 `eventId`로 중복을 거릅니다.

### `GET /api/client-config`

검색 화면이 시작할 때 읽는 설정입니다. `{"kakaoMapJsKey": "...", "devToolsEnabled": false}`. `devToolsEnabled`가 `true`면 화면이 충전소 상세에 테스트 빈자리 버튼을 보여 줍니다. 카카오맵 JavaScript 키는 브라우저에 그대로 쓰이는 공개 키이며, 카카오 개발자 콘솔에 등록한 도메인에서만 동작합니다.

## 빈자리 알림 구독

로그인 대신 브라우저가 만든 무작위 값(UUID 등 영문·숫자·하이픈 16~64자)을 `X-Client-Key` 헤더로 보내 구독자를 구분합니다. 헤더가 없거나 형식이 틀리면 400입니다. 동작 방식은 [ADR-010](adr/010-vacancy-notification-delivery.md)에 있습니다.

| 메서드 | 경로 | 설명 |
|---|---|---|
| GET | `/api/subscriptions` | 구독한 충전소 목록 (최근 구독 순. 다시 켠 구독은 그 시각 기준) |
| POST | `/api/subscriptions` | 구독. 본문 `{"stationId":"KP002507"}`. 새로 켜면 201, 이미 구독 중이면 200 |
| DELETE | `/api/subscriptions/{stationId}` | 구독 해지. 구독하지 않은 충전소여도 204 |
| GET | `/api/notifications` | 알림함: 보낸 알림 최근 20건 (최신 순) |

```json
{
  "items": [
    {
      "stationId": "KP002507",
      "stationName": "강남구청 공영주차장",
      "address": "서울특별시 강남구 학동로 426",
      "stationActive": true,
      "subscribedAt": "2026-11-22T05:58:30.120Z"
    }
  ]
}
```

| 상황 | 응답 |
|---|---|
| 없거나 운영 정보에서 사라진 충전소 | 404 |
| 구독 한도(충전소 20곳) 초과 | 409 |
| `stationId`가 비었음 | 400 (`errors.stationId`) |

- 같은 충전소 알림은 10분에 한 번만 보냅니다(이벤트 발생 시각 기준).
- 매칭된 뒤 발송 전에 해지하면 보내지 않습니다.
- 15분이 지난 빈자리와 개발 도구로 넣은 상태(`source=MANUAL`)는 알리지 않습니다. 개발 도구를 켠 환경(`DEV_TOOLS_ENABLED=true`)에서는 `MANUAL`도 알립니다. 이때는 10분 제한을 적용하지 않고, 실제 알림의 10분 계산(마지막 알림 시각)도 바꾸지 않습니다.

## 개발·시연 도구

`DEV_TOOLS_ENABLED=true`일 때만 존재합니다(기본 `false`). 사용법은 [시연 가이드](demo.md)를 참고하세요.

| 메서드 | 경로 | 설명 |
|---|---|---|
| POST | `/dev/chargers/{stationId}/{chargerId}/status` | 충전기 상태 주입. 본문 `{"status":"AVAILABLE"}`. 202 응답 |
| POST | `/dev/stations/{stationId}/test-vacancy` | 테스트 빈자리: 충전기를 충전중 → 충전대기로 바꿈. `?chargerId=`가 없으면 첫 충전기. **`X-Requested-With` 헤더 필수**(값은 무엇이든 됨). 202 응답 `{"stationId", "chargerId", "occurredAt"}` |
| GET | `/dev/vacancies` | 이 인스턴스가 켜진 뒤 발행된 빈자리 이벤트 최근 50건 |

상태 주입은 없는 충전기면 404, `status`가 없거나 정의되지 않은 값이면 400입니다. 테스트 빈자리는 없는 충전소·충전기면 404, `X-Requested-With` 헤더가 없으면 400입니다(다른 사이트가 몰래 부르지 못하게).

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8280/dev/stations/KP002507/test-vacancy -Headers @{ 'X-Requested-With' = 'manual' }
```

## Kafka 메시지

### `charger.status` (키: 충전소 ID)

```json
{
  "stationId": "KP002507",
  "chargerId": "01",
  "status": "AVAILABLE",
  "statusUpdatedAt": "2026-11-21T18:01:05Z",
  "collectedAt": "2026-11-21T18:01:05.123Z",
  "source": "CHANGE"
}
```

`source`: `CHANGE`(2분 수집), `SNAPSHOT`(전체 동기화), `MANUAL`(개발 도구)

### `charger.available` (키: 충전소 ID)

```json
{
  "eventId": "KP002507:01:1795284065000",
  "stationId": "KP002507",
  "chargerId": "01",
  "previousStatus": "CHARGING",
  "occurredAt": "2026-11-21T18:01:05Z",
  "source": "CHANGE"
}
```

- 최소 한 번 발행됩니다. 소비자는 `eventId`로 중복을 거릅니다.
- `source`가 `MANUAL`이면 개발 도구로 넣은 상태입니다. 알림은 개발 도구를 켠 환경에서만 보냅니다.

## RabbitMQ 메시지

### `notification.send.queue` (교환기 `notification.exchange`, 라우팅 키 `notification.send`)

```json
{
  "notificationId": 42,
  "eventId": "KP002507:01:1795284065000",
  "subscriptionId": 7,
  "subscriberClientKey": "3f1c...",
  "stationId": "KP002507",
  "stationName": "강남구청 공영주차장",
  "chargerId": "01",
  "occurredAt": "2026-11-21T18:01:05Z"
}
```

- 헤더 `x-notification-id`에 발송 기록 ID를 함께 담습니다. 본문을 읽지 못해도 최종 실패를 기록하기 위해서입니다.
- 3번 시도해도 실패하면 `notification.dlx`를 거쳐 `notification.send.dlq`로 갑니다.

## Redis Pub/Sub

채널 `notification.push`: `{"clientKey": "...", "notification": {...}}`. 모든 인스턴스가 구독하고, 자기에게 연결된 그 구독자의 SSE 연결로 `notification`(clientKey 제외)을 보냅니다.
