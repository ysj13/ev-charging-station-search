-- 충전소 검색용 인덱스

-- 이름·주소 부분 일치(ILIKE '%키워드%') 검색용 트라이그램 인덱스
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX idx_station_name_trgm ON station USING gin (name gin_trgm_ops) WHERE active;
CREATE INDEX idx_station_address_trgm ON station USING gin (address gin_trgm_ops) WHERE active;

-- 반경 검색: 위경도 범위(bounding box)로 먼저 거른다
CREATE INDEX idx_station_lat_lng ON station (latitude, longitude) WHERE active;
