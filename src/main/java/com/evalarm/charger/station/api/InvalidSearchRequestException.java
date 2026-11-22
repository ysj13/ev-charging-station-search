package com.evalarm.charger.station.api;

public class InvalidSearchRequestException extends RuntimeException {

    public InvalidSearchRequestException(String message) {
        super(message);
    }
}
