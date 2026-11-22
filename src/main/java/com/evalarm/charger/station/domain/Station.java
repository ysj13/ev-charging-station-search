package com.evalarm.charger.station.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import org.jspecify.annotations.Nullable;

@Entity
@Table(name = "station")
public class Station {

    @Id
    @Column(name = "station_id", length = 20)
    private String stationId;

    @Column(nullable = false, length = 200)
    private String name;

    @Column(nullable = false, length = 300)
    private String address;

    @Column(name = "location_detail", length = 300)
    private @Nullable String locationDetail;

    @Column(nullable = false)
    private double latitude;

    @Column(nullable = false)
    private double longitude;

    @Column(name = "use_time", length = 100)
    private @Nullable String useTime;

    @Column(name = "operator_id", length = 20)
    private @Nullable String operatorId;

    @Column(name = "operator_name", length = 100)
    private @Nullable String operatorName;

    @Column(name = "operator_call", length = 50)
    private @Nullable String operatorCall;

    @Column(nullable = false, length = 2)
    private String zcode;

    @Column(length = 5)
    private @Nullable String zscode;

    @Column(name = "parking_free")
    private @Nullable Boolean parkingFree;

    private @Nullable Boolean limited;

    @Column(name = "limit_detail", length = 300)
    private @Nullable String limitDetail;

    /** 공공데이터에서 사라진 충전소는 false (구독 참조 보존을 위해 삭제하지 않음) */
    @Column(nullable = false)
    private boolean active;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    /** JPA 전용 */
    protected Station() {
    }

    public Station(
            String stationId,
            String name,
            String address,
            @Nullable String locationDetail,
            double latitude,
            double longitude,
            @Nullable String useTime,
            @Nullable String operatorId,
            @Nullable String operatorName,
            @Nullable String operatorCall,
            String zcode,
            @Nullable String zscode,
            @Nullable Boolean parkingFree,
            @Nullable Boolean limited,
            @Nullable String limitDetail,
            boolean active,
            Instant createdAt,
            Instant updatedAt) {
        this.stationId = stationId;
        this.name = name;
        this.address = address;
        this.locationDetail = locationDetail;
        this.latitude = latitude;
        this.longitude = longitude;
        this.useTime = useTime;
        this.operatorId = operatorId;
        this.operatorName = operatorName;
        this.operatorCall = operatorCall;
        this.zcode = zcode;
        this.zscode = zscode;
        this.parkingFree = parkingFree;
        this.limited = limited;
        this.limitDetail = limitDetail;
        this.active = active;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    /** 선택 값은 비우고, 활성 상태와 현재 시각으로 만든다 */
    public Station(String stationId, String name, String address, double latitude, double longitude, String zcode) {
        this(stationId, name, address, null, latitude, longitude, null, null, null, null, zcode, null, null, null, null,
                true, Instant.now(), Instant.now());
    }

    public String getStationId() {
        return stationId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public @Nullable String getLocationDetail() {
        return locationDetail;
    }

    public void setLocationDetail(@Nullable String locationDetail) {
        this.locationDetail = locationDetail;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public @Nullable String getUseTime() {
        return useTime;
    }

    public void setUseTime(@Nullable String useTime) {
        this.useTime = useTime;
    }

    public @Nullable String getOperatorId() {
        return operatorId;
    }

    public void setOperatorId(@Nullable String operatorId) {
        this.operatorId = operatorId;
    }

    public @Nullable String getOperatorName() {
        return operatorName;
    }

    public void setOperatorName(@Nullable String operatorName) {
        this.operatorName = operatorName;
    }

    public @Nullable String getOperatorCall() {
        return operatorCall;
    }

    public void setOperatorCall(@Nullable String operatorCall) {
        this.operatorCall = operatorCall;
    }

    public String getZcode() {
        return zcode;
    }

    public void setZcode(String zcode) {
        this.zcode = zcode;
    }

    public @Nullable String getZscode() {
        return zscode;
    }

    public void setZscode(@Nullable String zscode) {
        this.zscode = zscode;
    }

    public @Nullable Boolean getParkingFree() {
        return parkingFree;
    }

    public void setParkingFree(@Nullable Boolean parkingFree) {
        this.parkingFree = parkingFree;
    }

    public @Nullable Boolean getLimited() {
        return limited;
    }

    public void setLimited(@Nullable Boolean limited) {
        this.limited = limited;
    }

    public @Nullable String getLimitDetail() {
        return limitDetail;
    }

    public void setLimitDetail(@Nullable String limitDetail) {
        this.limitDetail = limitDetail;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
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
}
