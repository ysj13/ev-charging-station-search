package com.evalarm.charger.notification.domain;

/**
 * 발송 상태.
 * PENDING: 매칭되어 기록됨 → QUEUED: RabbitMQ에 적재 확인 → SENT: 발송 완료 / FAILED: 재시도 끝에 실패(DLQ)
 * CANCELLED: 발송 전에 구독을 해지해 보내지 않음
 */
public enum NotificationStatus {
    PENDING,
    QUEUED,
    SENT,
    FAILED,
    CANCELLED
}
