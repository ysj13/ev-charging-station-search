package com.evalarm.charger.stream;

public class TooManyStreamConnectionsException extends RuntimeException {

    public TooManyStreamConnectionsException() {
        super("실시간 연결이 너무 많습니다. 잠시 뒤 다시 시도해 주세요");
    }
}
