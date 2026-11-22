-- 공공데이터에서 사라진(폐지된) 충전소는 구독 등에서 참조될 수 있어 지우지 않고 비활성으로 표시한다
ALTER TABLE station ADD COLUMN active BOOLEAN NOT NULL DEFAULT TRUE;

CREATE INDEX idx_station_active_zcode ON station (zcode, zscode) WHERE active;
