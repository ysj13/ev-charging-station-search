-- 알림 발송: 알림함에 표시할 빈자리 발생 시각, 알림함 조회와 미발송 재처리용 인덱스
ALTER TABLE notification_log ADD COLUMN occurred_at TIMESTAMP(6) WITH TIME ZONE;

-- 알림함: 구독별 최근 알림
CREATE INDEX idx_notification_log_subscription_created ON notification_log (subscription_id, created_at DESC);

-- 매칭 재처리: 같은 이벤트의 미발송(PENDING) 건 조회
CREATE INDEX idx_notification_log_event_pending ON notification_log (event_id) WHERE status = 'PENDING';
