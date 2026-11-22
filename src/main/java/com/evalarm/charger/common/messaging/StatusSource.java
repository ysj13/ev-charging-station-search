package com.evalarm.charger.common.messaging;

/** charger.status 메시지가 어디서 왔는지 */
public enum StatusSource {
    /** getChargerStatus 주기 수집: 최근 바뀐 충전기만 */
    CHANGE,

    /** getChargerInfo 전체 동기화: 모든 충전기의 현재 상태 (재기동·장기 장애 후 따라잡기용) */
    SNAPSHOT,

    /** 개발·시연용 상태 주입기로 넣은 상태. 실제 데이터가 아니므로 지표와 이벤트에서 구분한다 */
    MANUAL,
}
