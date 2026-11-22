package com.evalarm.charger.notification.domain;

import com.evalarm.charger.subscription.domain.Subscription;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import java.time.Instant;
import org.jspecify.annotations.Nullable;

/** (subscription, eventId) 유니크 제약으로 같은 이벤트의 중복 발송을 막는다 */
@Entity
@Table(
        name = "notification_log",
        uniqueConstraints = {
            @UniqueConstraint(name = "uk_notification_subscription_event", columnNames = {"subscription_id", "event_id"}),
        })
public class NotificationLog {

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "subscription_id", nullable = false)
    private Subscription subscription;

    @Column(name = "event_id", nullable = false, length = 100)
    private String eventId;

    @Column(name = "station_id", nullable = false, length = 20)
    private String stationId;

    @Column(name = "charger_id", nullable = false, length = 10)
    private String chargerId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private NotificationStatus status;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    @Column(name = "sent_at")
    private @Nullable Instant sentAt;

    /** 빈자리가 난 시각 (알림함 표시용) */
    @Column(name = "occurred_at")
    private @Nullable Instant occurredAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private @Nullable Long id;

    /** JPA 전용 */
    protected NotificationLog() {
    }

    public NotificationLog(
            Subscription subscription,
            String eventId,
            String stationId,
            String chargerId,
            NotificationStatus status,
            int attemptCount,
            @Nullable Instant sentAt,
            @Nullable Instant occurredAt,
            Instant createdAt) {
        this.subscription = subscription;
        this.eventId = eventId;
        this.stationId = stationId;
        this.chargerId = chargerId;
        this.status = status;
        this.attemptCount = attemptCount;
        this.sentAt = sentAt;
        this.occurredAt = occurredAt;
        this.createdAt = createdAt;
    }

    /** PENDING, 시도 0회, 현재 시각으로 만든다 */
    public NotificationLog(Subscription subscription, String eventId, String stationId, String chargerId) {
        this(subscription, eventId, stationId, chargerId, NotificationStatus.PENDING, 0, null, null, Instant.now());
    }

    public Subscription getSubscription() {
        return subscription;
    }

    public String getEventId() {
        return eventId;
    }

    public String getStationId() {
        return stationId;
    }

    public String getChargerId() {
        return chargerId;
    }

    public NotificationStatus getStatus() {
        return status;
    }

    public void setStatus(NotificationStatus status) {
        this.status = status;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public void setAttemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
    }

    public @Nullable Instant getSentAt() {
        return sentAt;
    }

    public void setSentAt(@Nullable Instant sentAt) {
        this.sentAt = sentAt;
    }

    public @Nullable Instant getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(@Nullable Instant occurredAt) {
        this.occurredAt = occurredAt;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public @Nullable Long getId() {
        return id;
    }
}
