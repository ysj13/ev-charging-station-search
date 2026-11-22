package com.evalarm.charger.common.config;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ClockConfig {

    /** 시간 의존 로직을 테스트에서 고정된 시계로 검증할 수 있도록 주입받는다 */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
