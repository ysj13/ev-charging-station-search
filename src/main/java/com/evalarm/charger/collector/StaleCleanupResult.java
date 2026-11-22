package com.evalarm.charger.collector;

public record StaleCleanupResult(int deletedChargers, int deactivatedStations) {
}
