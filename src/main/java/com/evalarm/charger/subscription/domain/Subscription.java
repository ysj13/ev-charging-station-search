package com.evalarm.charger.subscription.domain;

import com.evalarm.charger.station.domain.Station;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import java.time.Duration;
import java.time.Instant;
import org.jspecify.annotations.Nullable;

@Entity
@Table(
        name = "subscription",
        uniqueConstraints = {
            @UniqueConstraint(name = "uk_subscription_subscriber_station", columnNames = {"subscriber_id", "station_id"}),
        })
public class Subscription {

    public static final int DEFAULT_COOLDOWN_MINUTES = 10;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "subscriber_id", nullable = false)
    private Subscriber subscriber;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "station_id", nullable = false)
    private Station station;

    @Column(nullable = false)
    private boolean active;

    @Column(name = "cooldown_minutes", nullable = false)
    private int cooldownMinutes;

    @Column(name = "last_notified_at")
    private @Nullable Instant lastNotifiedAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private @Nullable Long id;

    /** JPA 전용 */
    protected Subscription() {
    }

    public Subscription(
            Subscriber subscriber,
            Station station,
            boolean active,
            int cooldownMinutes,
            @Nullable Instant lastNotifiedAt,
            Instant createdAt) {
        this.subscriber = subscriber;
        this.station = station;
        this.active = active;
        this.cooldownMinutes = cooldownMinutes;
        this.lastNotifiedAt = lastNotifiedAt;
        this.createdAt = createdAt;
    }

    /** 활성, 기본 쿨다운, 알림 이력 없음, 현재 시각으로 만든다 */
    public Subscription(Subscriber subscriber, Station station) {
        this(subscriber, station, true, DEFAULT_COOLDOWN_MINUTES, null, Instant.now());
    }

    /** 쿨다운이 지나 다시 알림을 보낼 수 있는지 */
    public boolean isNotifiable(Instant now) {
        if (!active) {
            return false;
        }
        Instant last = lastNotifiedAt;
        if (last == null) {
            return true;
        }
        return Duration.between(last, now).compareTo(Duration.ofMinutes(cooldownMinutes)) >= 0;
    }

    public Subscriber getSubscriber() {
        return subscriber;
    }

    public Station getStation() {
        return station;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public int getCooldownMinutes() {
        return cooldownMinutes;
    }

    public void setCooldownMinutes(int cooldownMinutes) {
        this.cooldownMinutes = cooldownMinutes;
    }

    public @Nullable Instant getLastNotifiedAt() {
        return lastNotifiedAt;
    }

    public void setLastNotifiedAt(@Nullable Instant lastNotifiedAt) {
        this.lastNotifiedAt = lastNotifiedAt;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public @Nullable Long getId() {
        return id;
    }
}
