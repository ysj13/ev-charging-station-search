package com.evalarm.charger.subscription.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import org.jspecify.annotations.Nullable;

/** 로그인 없이 브라우저가 발급한 clientKey로 식별하는 구독자 */
@Entity
@Table(name = "subscriber")
public class Subscriber {

    @Column(name = "client_key", nullable = false, unique = true, length = 64)
    private String clientKey;

    @Column(length = 50)
    private @Nullable String nickname;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private @Nullable Long id;

    /** JPA 전용 */
    protected Subscriber() {
    }

    public Subscriber(String clientKey, @Nullable String nickname, Instant createdAt) {
        this.clientKey = clientKey;
        this.nickname = nickname;
        this.createdAt = createdAt;
    }

    /** 별명 없이 현재 시각으로 만든다 */
    public Subscriber(String clientKey) {
        this(clientKey, null, Instant.now());
    }

    public String getClientKey() {
        return clientKey;
    }

    public @Nullable String getNickname() {
        return nickname;
    }

    public void setNickname(@Nullable String nickname) {
        this.nickname = nickname;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public @Nullable Long getId() {
        return id;
    }
}
