package com.evalarm.charger.collector.client;

import java.util.List;

public record PublicApiPage<T>(int totalCount, int pageNo, List<T> items) {
}
