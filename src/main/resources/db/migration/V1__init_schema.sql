-- 충전소: 공공데이터 API(한국환경공단_전기자동차 충전소 정보)의 충전소 단위 정보
CREATE TABLE station (
    station_id      VARCHAR(20)      PRIMARY KEY,           -- statId
    name            VARCHAR(200)     NOT NULL,              -- statNm
    address         VARCHAR(300)     NOT NULL,              -- addr
    location_detail VARCHAR(300),                           -- location
    latitude        DOUBLE PRECISION NOT NULL,              -- lat
    longitude       DOUBLE PRECISION NOT NULL,              -- lng
    use_time        VARCHAR(100),                           -- useTime
    operator_id     VARCHAR(20),                            -- busiId
    operator_name   VARCHAR(100),                           -- busiNm
    operator_call   VARCHAR(50),                            -- busiCall
    zcode           VARCHAR(2)       NOT NULL,              -- 시도 코드
    zscode          VARCHAR(5),                             -- 시군구 코드
    parking_free    BOOLEAN,                                -- parkingFree
    limited         BOOLEAN,                                -- limitYn
    limit_detail    VARCHAR(300),                           -- limitDetail
    created_at      TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    updated_at      TIMESTAMP(6) WITH TIME ZONE NOT NULL
);

CREATE INDEX idx_station_zcode_zscode ON station (zcode, zscode);

-- 충전기: 충전소 안의 개별 충전기와 현재 상태 스냅샷 (조회용. 변경 감지는 Redis에서 수행)
CREATE TABLE charger (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    station_id        VARCHAR(20)  NOT NULL REFERENCES station (station_id),
    charger_id        VARCHAR(10)  NOT NULL,                -- chgerId
    charger_type      VARCHAR(10),                          -- chgerType (코드값 원본)
    output_kw         INTEGER,                              -- output
    status            VARCHAR(20)  NOT NULL,                -- ChargerStatus enum
    status_updated_at TIMESTAMP(6) WITH TIME ZONE,          -- statUpdDt
    created_at        TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    updated_at        TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    CONSTRAINT uk_charger_station_charger UNIQUE (station_id, charger_id)
);

-- 구독자: 로그인 없이 브라우저가 발급한 식별자(clientKey)로 구분
CREATE TABLE subscriber (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    client_key VARCHAR(64)  NOT NULL,
    nickname   VARCHAR(50),
    created_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    CONSTRAINT uk_subscriber_client_key UNIQUE (client_key)
);

-- 구독: 구독자가 특정 충전소의 빈자리 알림을 신청
CREATE TABLE subscription (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subscriber_id    BIGINT       NOT NULL REFERENCES subscriber (id),
    station_id       VARCHAR(20)  NOT NULL REFERENCES station (station_id),
    active           BOOLEAN      NOT NULL,
    cooldown_minutes INTEGER      NOT NULL,                 -- 같은 충전소 알림 최소 간격
    last_notified_at TIMESTAMP(6) WITH TIME ZONE,
    created_at       TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    CONSTRAINT uk_subscription_subscriber_station UNIQUE (subscriber_id, station_id)
);

-- 매칭 시 "이 충전소의 활성 구독자" 조회용
CREATE INDEX idx_subscription_station_active ON subscription (station_id) WHERE active;

-- 알림 발송 이력: (subscription_id, event_id) 유니크로 중복 발송 방지(멱등성)
CREATE TABLE notification_log (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subscription_id BIGINT       NOT NULL REFERENCES subscription (id),
    event_id        VARCHAR(100) NOT NULL,
    station_id      VARCHAR(20)  NOT NULL,
    charger_id      VARCHAR(10)  NOT NULL,
    status          VARCHAR(20)  NOT NULL,                  -- NotificationStatus enum
    attempt_count   INTEGER      NOT NULL,
    sent_at         TIMESTAMP(6) WITH TIME ZONE,
    created_at      TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    CONSTRAINT uk_notification_subscription_event UNIQUE (subscription_id, event_id)
);
