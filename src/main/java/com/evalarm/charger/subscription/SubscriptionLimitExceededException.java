package com.evalarm.charger.subscription;

public class SubscriptionLimitExceededException extends RuntimeException {

    public SubscriptionLimitExceededException(int limit) {
        super("충전소는 최대 " + limit + "곳까지 구독할 수 있습니다. 다른 구독을 해지한 뒤 다시 시도해 주세요");
    }
}
