package com.evalarm.charger.station.domain;

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

@Entity
@Table(
        name = "charger",
        uniqueConstraints = @UniqueConstraint(name = "uk_charger_station_charger", columnNames = {"station_id", "charger_id"}))
public class Charger {

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "station_id", nullable = false)
    private Station station;

    @Column(name = "charger_id", nullable = false, length = 10)
    private String chargerId;

    @Column(name = "charger_type", length = 10)
    private @Nullable String chargerType;

    @Column(name = "output_kw")
    private @Nullable Integer outputKw;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private ChargerStatus status;

    @Column(name = "status_updated_at")
    private @Nullable Instant statusUpdatedAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private @Nullable Long id;

    /** JPA 전용 */
    protected Charger() {
    }

    public Charger(
            Station station,
            String chargerId,
            @Nullable String chargerType,
            @Nullable Integer outputKw,
            ChargerStatus status,
            @Nullable Instant statusUpdatedAt,
            Instant createdAt,
            Instant updatedAt) {
        this.station = station;
        this.chargerId = chargerId;
        this.chargerType = chargerType;
        this.outputKw = outputKw;
        this.status = status;
        this.statusUpdatedAt = statusUpdatedAt;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    /** 선택 값은 비우고 현재 시각으로 만든다 */
    public Charger(Station station, String chargerId, ChargerStatus status) {
        this(station, chargerId, null, null, status, null, Instant.now(), Instant.now());
    }

    public Station getStation() {
        return station;
    }

    public String getChargerId() {
        return chargerId;
    }

    public @Nullable String getChargerType() {
        return chargerType;
    }

    public void setChargerType(@Nullable String chargerType) {
        this.chargerType = chargerType;
    }

    public @Nullable Integer getOutputKw() {
        return outputKw;
    }

    public void setOutputKw(@Nullable Integer outputKw) {
        this.outputKw = outputKw;
    }

    public ChargerStatus getStatus() {
        return status;
    }

    public void setStatus(ChargerStatus status) {
        this.status = status;
    }

    public @Nullable Instant getStatusUpdatedAt() {
        return statusUpdatedAt;
    }

    public void setStatusUpdatedAt(@Nullable Instant statusUpdatedAt) {
        this.statusUpdatedAt = statusUpdatedAt;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public @Nullable Long getId() {
        return id;
    }
}
