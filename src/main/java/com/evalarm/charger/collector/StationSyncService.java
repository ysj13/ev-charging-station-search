package com.evalarm.charger.collector;

import static com.evalarm.charger.collector.client.PublicApiValues.blankToNull;
import static com.evalarm.charger.collector.client.PublicApiValues.parseDateTime;
import static com.evalarm.charger.collector.client.PublicApiValues.parseYn;

import com.evalarm.charger.collector.client.ChargerInfoItem;
import com.evalarm.charger.collector.client.EvChargerApiClient;
import com.evalarm.charger.collector.client.PublicApiPage;
import com.evalarm.charger.common.config.AppProperties;
import com.evalarm.charger.common.messaging.ChargerStatusMessage;
import com.evalarm.charger.common.messaging.StatusSource;
import com.evalarm.charger.common.text.Whitespace;
import com.evalarm.charger.station.domain.ChargerKey;
import com.evalarm.charger.station.domain.ChargerStatus;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * getChargerInfo로 수집 지역의 충전소·충전기 정보를 동기화한다.
 * <ul>
 *   <li>DB: 검색 기능의 기준 데이터</li>
 *   <li>Kafka: 모든 충전기의 현재 상태를 SNAPSHOT으로 발행해, 재기동이나 장기 장애로 놓친 상태 변경을 따라잡게 한다
 *   (getChargerStatus는 최근 약 10분까지만 조회할 수 있다)</li>
 * </ul>
 *
 * <p>한 번에 하나의 인스턴스에서만 실행하는 것을 전제로 한다(현재 단일 인스턴스 배포).
 * 겹쳐 실행되더라도 updated_at은 GREATEST로 기록해 정상 데이터가 지워지지 않게 했다.
 */
@Service
public class StationSyncService {

    private static final Logger log = LoggerFactory.getLogger(StationSyncService.class);

    private static final int MAX_STATION_ID_LENGTH = 20;
    private static final int MAX_CHARGER_ID_LENGTH = 10;
    private static final long SEND_TIMEOUT_SECONDS = 60L;

    // 대한민국 영역(제주·울릉·독도 포함)을 넉넉하게 감싸는 범위. 0, NaN, Infinity는 걸러진다
    private static final double KOREA_LATITUDE_MIN = 33.0;
    private static final double KOREA_LATITUDE_MAX = 39.0;
    private static final double KOREA_LONGITUDE_MIN = 124.0;
    private static final double KOREA_LONGITUDE_MAX = 132.0;

    private final EvChargerApiClient apiClient;
    private final StationUpsertRepository upsertRepository;
    private final TransactionTemplate transactionTemplate;
    private final KafkaTemplate<String, ChargerStatusMessage> kafkaTemplate;
    private final AppProperties properties;
    private final Clock clock;

    /** 안전장치 때문에 정리하지 못하고 남아 있는 충전기 수. 0이 아니면 사람이 확인해야 한다 */
    private final AtomicInteger pendingCleanup = new AtomicInteger(0);

    public StationSyncService(
            EvChargerApiClient apiClient,
            StationUpsertRepository upsertRepository,
            TransactionTemplate transactionTemplate,
            KafkaTemplate<String, ChargerStatusMessage> kafkaTemplate,
            AppProperties properties,
            Clock clock,
            MeterRegistry meterRegistry) {
        this.apiClient = apiClient;
        this.upsertRepository = upsertRepository;
        this.transactionTemplate = transactionTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.clock = clock;
        meterRegistry.gauge("collector.station.cleanup.pending", pendingCleanup);
    }

    public StationSyncResult syncAll() throws InterruptedException, ExecutionException, TimeoutException {
        // 모든 페이지의 updated_at을 이 시각 하나로 기록한다.
        // 정리 기준(updated_at < syncStartedAt)이 동기화 도중 시계 보정의 영향을 받지 않게 하기 위해서다
        Instant syncStartedAt = clock.instant();
        int pageSize = properties.collector().pageSize();
        String zcode = properties.publicApi().zcode();
        Set<String> stationIds = new HashSet<>();
        Set<ChargerKey> seenKeys = new HashSet<>();
        List<Integer> failedPages = new ArrayList<>();
        List<Integer> inconsistentPages = new ArrayList<>();
        int received = 0;
        int chargers = 0;
        int removed = 0;
        int skipped = 0;

        // 첫 페이지는 전체 건수를 알아야 하므로 실패하면 동기화 전체를 실패시킨다
        PublicApiPage<ChargerInfoItem> first = apiClient.fetchChargerInfo(1, pageSize);
        int lastPage = Math.max((first.totalCount() + pageSize - 1) / pageSize, 1);

        for (int pageNo = 1; pageNo <= lastPage; pageNo++) {
            try {
                PublicApiPage<ChargerInfoItem> page = pageNo == 1 ? first : apiClient.fetchChargerInfo(pageNo, pageSize);
                // 동기화 도중 원본 건수가 바뀌면 페이지 경계가 밀려 누락·중복이 생길 수 있다
                if (page.totalCount() != first.totalCount()) {
                    inconsistentPages.add(pageNo);
                }
                PageResult result = savePage(page.items(), syncStartedAt);
                received += page.items().size();
                seenKeys.addAll(result.seenKeys());
                stationIds.addAll(result.stationIds());
                chargers += result.chargers();
                removed += result.removed();
                skipped += result.skipped();
                log.info("충전소 동기화 {}/{}페이지: 충전기 {}건", pageNo, lastPage, result.chargers());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) {
                    throw e;
                }
                // 한 페이지가 실패해도 나머지 페이지는 계속 반영한다. 실패 페이지는 재시도에서 다시 맞춰진다
                failedPages.add(pageNo);
                log.error("충전소 동기화 {}/{}페이지 실패: {}", pageNo, lastPage, e.getMessage(), e);
            }
        }

        String skipReason = cleanupSkipReason(
                first.totalCount(), received, seenKeys.size(), failedPages, inconsistentPages, zcode, syncStartedAt);
        StaleCleanupResult cleanup = null;
        if (skipReason == null) {
            cleanup = transactionTemplate.execute(status -> upsertRepository.cleanup(zcode, syncStartedAt));
            pendingCleanup.set(0);
        }

        return new StationSyncResult(
                first.totalCount(),
                received,
                seenKeys.size(),
                stationIds.size(),
                chargers,
                removed,
                skipped,
                lastPage,
                failedPages,
                cleanup,
                skipReason);
    }

    /**
     * 정상 데이터를 대량으로 지우지 않도록, 응답이 완전하다고 확신할 수 있을 때만 정리한다.
     * <ul>
     *   <li>실패한 페이지가 없을 것 (실패는 재시도로 해결되므로 경보 대상 아님)</li>
     *   <li>받은 건수와 고유 충전기 수가 모두 totalCount와 같고, 모든 페이지의 totalCount가 같을 것
     *   (빈 응답, 잘린 페이지, 동기화 도중 원본 변경으로 인한 페이지 경계 밀림 방지)</li>
     *   <li>지울 충전기가 기존의 일정 비율 이하일 것 (원본 이상 시 대량 삭제 방지)</li>
     * </ul>
     *
     * <p>실패 페이지 외의 이유로 건너뛰면 저절로 풀리지 않을 수 있어 ERROR 로그와 지표로 알린다.
     */
    private @Nullable String cleanupSkipReason(
            int totalCount,
            int received,
            int uniqueKeys,
            List<Integer> failedPages,
            List<Integer> inconsistentPages,
            String zcode,
            Instant syncStartedAt) {
        if (!failedPages.isEmpty()) {
            String reason = "실패한 페이지 " + failedPages;
            log.warn("사라진 충전기 정리를 건너뜁니다: {}", reason);
            return reason;
        }

        int stale = upsertRepository.countStaleChargers(zcode, syncStartedAt);
        String reason;
        if (totalCount <= 0) {
            reason = "totalCount가 0";
        } else if (!inconsistentPages.isEmpty()) {
            reason = "동기화 도중 totalCount가 바뀐 페이지 " + inconsistentPages;
        } else if (received != totalCount) {
            reason = "받은 건수(" + received + ")가 totalCount(" + totalCount + ")와 다름";
        } else if (uniqueKeys != totalCount) {
            reason = "고유 충전기 수(" + uniqueKeys + ")가 totalCount(" + totalCount + ")와 다름";
        } else {
            int existing = upsertRepository.countChargers(zcode);
            int maxStale = (int) (existing * properties.collector().maxStaleRatio());
            reason = stale > maxStale
                    ? "지울 충전기 " + stale + "건이 허용치(" + maxStale + "건, 기존 " + existing + "건)를 넘음"
                    : null;
        }
        if (reason == null) {
            return null;
        }

        pendingCleanup.set(stale);
        log.error(
                "사라진 충전기 정리를 건너뜁니다: {}. 정리 대기 {}건. 원본이 정상이라면 app.collector.max-stale-ratio를 일시적으로 올려 다시 동기화하세요.",
                reason, stale);
        return reason;
    }

    private record PageResult(
            Set<ChargerKey> seenKeys,
            Set<String> stationIds,
            int chargers,
            int removed,
            int skipped) {
    }

    private PageResult savePage(List<ChargerInfoItem> items, Instant syncStartedAt)
            throws InterruptedException, ExecutionException, TimeoutException {
        List<ChargerInfoItem> removedItems = new ArrayList<>();
        List<ChargerInfoItem> liveItems = new ArrayList<>();
        for (ChargerInfoItem item : items) {
            (Boolean.TRUE.equals(parseYn(item.delYn())) ? removedItems : liveItems).add(item);
        }
        List<ChargerInfoItem> valid = new ArrayList<>();
        List<ChargerInfoItem> invalid = new ArrayList<>();
        for (ChargerInfoItem item : liveItems) {
            (isUsable(item) ? valid : invalid).add(item);
        }
        // 응답에는 있지만 값이 이상해 저장하지 않은 충전기도 "본 것"으로 표시해 정리 대상에서 뺀다.
        // delYn=Y 충전기는 표시하지 않으므로 정리 단계에서 (같은 안전장치를 거쳐) 지워진다
        List<ChargerKey> seenInvalidKeys = new ArrayList<>();
        for (ChargerInfoItem item : invalid) {
            ChargerKey key = chargerKeyOrNull(item);
            if (key != null) {
                seenInvalidKeys.add(key);
            }
        }

        Map<String, ChargerInfoItem> firstByStation = new LinkedHashMap<>();
        for (ChargerInfoItem item : valid) {
            firstByStation.putIfAbsent(Whitespace.trim(item.statId()), item);
        }
        List<StationRow> stationRows = new ArrayList<>(firstByStation.size());
        for (ChargerInfoItem item : firstByStation.values()) {
            stationRows.add(toStationRow(item));
        }
        List<ChargerRow> chargerRows = new ArrayList<>(valid.size());
        for (ChargerInfoItem item : valid) {
            chargerRows.add(toChargerRow(item));
        }

        // 페이지 단위로 커밋한다
        transactionTemplate.executeWithoutResult(status -> {
            upsertRepository.upsertStations(stationRows, syncStartedAt);
            upsertRepository.upsertChargers(chargerRows, syncStartedAt);
            upsertRepository.touchChargers(seenInvalidKeys, syncStartedAt);
        });

        publishSnapshot(chargerRows);

        Set<ChargerKey> seenKeys = new HashSet<>();
        for (ChargerInfoItem item : items) {
            ChargerKey key = chargerKeyOrNull(item);
            if (key != null) {
                seenKeys.add(key);
            }
        }
        Set<String> stationIds = new HashSet<>();
        for (StationRow row : stationRows) {
            stationIds.add(row.stationId());
        }
        return new PageResult(seenKeys, stationIds, chargerRows.size(), removedItems.size(), invalid.size());
    }

    private void publishSnapshot(List<ChargerRow> rows)
            throws InterruptedException, ExecutionException, TimeoutException {
        String topic = properties.kafka().topics().chargerStatus().name();
        Instant collectedAt = clock.instant();
        List<CompletableFuture<?>> sends = new ArrayList<>(rows.size());
        for (ChargerRow row : rows) {
            ChargerStatusMessage message = new ChargerStatusMessage(
                    row.stationId(),
                    row.chargerId(),
                    row.status(),
                    row.statusUpdatedAt(),
                    collectedAt,
                    StatusSource.SNAPSHOT);
            sends.add(kafkaTemplate.send(topic, row.stationId(), message));
        }
        CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /** 필수 값, ID 길이, 좌표 범위를 검사해 DB 제약을 어기거나 지도에 잘못 찍힐 항목을 걸러낸다 */
    private static boolean isUsable(ChargerInfoItem item) {
        if (chargerKeyOrNull(item) == null) {
            return false;
        }
        if (blankToNull(item.statNm()) == null || blankToNull(item.zcode()) == null) {
            return false;
        }
        Double latitude = parseDoubleOrNull(item.lat());
        if (latitude == null) {
            return false;
        }
        Double longitude = parseDoubleOrNull(item.lng());
        if (longitude == null) {
            return false;
        }
        return KOREA_LATITUDE_MIN <= latitude && latitude <= KOREA_LATITUDE_MAX
                && KOREA_LONGITUDE_MIN <= longitude && longitude <= KOREA_LONGITUDE_MAX;
    }

    private static @Nullable ChargerKey chargerKeyOrNull(ChargerInfoItem item) {
        String stationId = blankToNull(item.statId());
        if (stationId == null) {
            return null;
        }
        String chargerId = blankToNull(item.chgerId());
        if (chargerId == null) {
            return null;
        }
        if (stationId.length() > MAX_STATION_ID_LENGTH || chargerId.length() > MAX_CHARGER_ID_LENGTH) {
            return null;
        }
        return new ChargerKey(stationId, chargerId);
    }

    // 아래 두 변환은 isUsable을 통과한 항목에만 쓰므로 statId, chgerId, statNm, lat, lng, zcode가 있다
    private static StationRow toStationRow(ChargerInfoItem item) {
        List<String> addressParts = new ArrayList<>(2);
        String addr = blankToNull(item.addr());
        if (addr != null) {
            addressParts.add(addr);
        }
        String addrDetail = blankToNull(item.addrDetail());
        if (addrDetail != null) {
            addressParts.add(addrDetail);
        }
        return new StationRow(
                Whitespace.trim(Objects.requireNonNull(item.statId())),
                take(Whitespace.trim(Objects.requireNonNull(item.statNm())), 200),
                take(String.join(" ", addressParts), 300),
                takeOrNull(blankToNull(item.location()), 300),
                Double.parseDouble(Whitespace.trim(Objects.requireNonNull(item.lat()))),
                Double.parseDouble(Whitespace.trim(Objects.requireNonNull(item.lng()))),
                takeOrNull(blankToNull(item.useTime()), 100),
                takeOrNull(blankToNull(item.busiId()), 20),
                takeOrNull(blankToNull(item.busiNm()), 100),
                takeOrNull(blankToNull(item.busiCall()), 50),
                take(Whitespace.trim(Objects.requireNonNull(item.zcode())), 2),
                takeOrNull(blankToNull(item.zscode()), 5),
                parseYn(item.parkingFree()),
                parseYn(item.limitYn()),
                takeOrNull(blankToNull(item.limitDetail()), 300));
    }

    private static ChargerRow toChargerRow(ChargerInfoItem item) {
        return new ChargerRow(
                Whitespace.trim(Objects.requireNonNull(item.statId())),
                Whitespace.trim(Objects.requireNonNull(item.chgerId())),
                takeOrNull(blankToNull(item.chgerType()), 10),
                parseIntOrNull(item.output()),
                ChargerStatus.fromCode(item.stat()),
                parseDateTime(item.statUpdDt()));
    }

    /** 앞뒤 공백을 뺀 값을 실수로 읽는다. 없거나 형식이 틀리면 null */
    private static @Nullable Double parseDoubleOrNull(@Nullable String value) {
        if (value == null) {
            return null;
        }
        try {
            return Double.parseDouble(Whitespace.trim(value));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** 앞뒤 공백을 뺀 값을 정수로 읽는다. 없거나 형식이 틀리면 null */
    private static @Nullable Integer parseIntOrNull(@Nullable String value) {
        if (value == null) {
            return null;
        }
        try {
            return Integer.parseInt(Whitespace.trim(value));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String take(String value, int length) {
        return value.length() <= length ? value : value.substring(0, length);
    }

    private static @Nullable String takeOrNull(@Nullable String value, int length) {
        return value != null ? take(value, length) : null;
    }
}
