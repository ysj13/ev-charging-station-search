-- 시군구 코드 단독 검색용. 기존 (zcode, zscode) 인덱스는 선행 컬럼(zcode) 조건이 없으면 쓰이지 않는다
CREATE INDEX idx_station_zscode ON station (zscode) WHERE active;
