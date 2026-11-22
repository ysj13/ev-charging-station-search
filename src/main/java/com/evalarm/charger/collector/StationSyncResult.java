package com.evalarm.charger.collector;

import java.util.List;
import org.jspecify.annotations.Nullable;

public record StationSyncResult(
        int totalCount,
        int received,
        /** 응답에 나온 고유 충전기 수 (중복 제외) */
        int uniqueChargers,
        int stations,
        int chargers,
        /** 응답에서 delYn=Y로 표시된 충전기 수 (정리 단계에서 지워진다) */
        int removedInSource,
        int skipped,
        int pages,
        List<Integer> failedPages,
        @Nullable StaleCleanupResult cleanup,
        @Nullable String cleanupSkippedReason) {

    /** 모든 페이지를 받아 반영했는지. 재시도 판단 기준 */
    public boolean isComplete() {
        return failedPages.isEmpty();
    }
}
