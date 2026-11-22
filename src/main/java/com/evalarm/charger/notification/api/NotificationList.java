package com.evalarm.charger.notification.api;

import com.evalarm.charger.notification.NotificationView;
import java.util.List;

public record NotificationList(List<NotificationView> items) {
}
