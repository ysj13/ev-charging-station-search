package com.evalarm.charger.subscription;

import com.evalarm.charger.common.config.AppProperties;
import com.evalarm.charger.station.api.StationNotFoundException;
import com.evalarm.charger.subscription.domain.Subscription;
import java.time.Clock;
import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class SubscriptionService {

    private final SubscriptionRepository repository;
    private final AppProperties properties;
    private final Clock clock;

    public SubscriptionService(SubscriptionRepository repository, AppProperties properties, Clock clock) {
        this.repository = repository;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * 충전소 빈자리 알림을 구독한다. 이미 구독 중이면 그대로 둔다.
     *
     * @return 새로 켰으면 true
     */
    @Transactional
    public boolean subscribe(String clientKey, String stationId) {
        if (!repository.existsActiveStation(stationId)) {
            throw new StationNotFoundException(stationId);
        }
        long subscriberId = repository.lockOrCreateSubscriber(clientKey, clock.instant());
        if (repository.isActive(subscriberId, stationId)) {
            return false;
        }
        int limit = properties.notification().maxSubscriptionsPerClient();
        if (repository.countActive(subscriberId) >= limit) {
            throw new SubscriptionLimitExceededException(limit);
        }
        repository.activate(subscriberId, stationId, Subscription.DEFAULT_COOLDOWN_MINUTES, clock.instant());
        return true;
    }

    /** 구독을 해지한다. 구독하지 않은 충전소여도 오류 없이 끝난다 */
    @Transactional
    public void unsubscribe(String clientKey, String stationId) {
        repository.deactivate(clientKey, stationId);
    }

    @Transactional(readOnly = true)
    public List<SubscriptionView> list(String clientKey) {
        return repository.findActive(clientKey);
    }
}
