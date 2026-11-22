package com.evalarm.charger.station.query;

import java.util.List;

public record StationSearchResult(List<StationSummary> items, int page, int size, long totalCount) {
}
