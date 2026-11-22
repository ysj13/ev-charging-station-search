package com.evalarm.charger.subscription.api;

import com.evalarm.charger.subscription.SubscriptionView;
import java.util.List;

public record SubscriptionList(List<SubscriptionView> items) {
}
