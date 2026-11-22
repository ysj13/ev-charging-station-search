package com.evalarm.charger.common.config;

import java.net.http.HttpClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration
public class HttpClientConfig {

    /** 공공데이터 API 전용 RestClient. 외부 API 지연이 수집 스케줄 전체를 막지 않도록 타임아웃을 둔다. */
    @Bean
    public RestClient evChargerRestClient(RestClient.Builder builder, AppProperties properties) {
        HttpClient httpClient = HttpClient.newBuilder()
                // 공공데이터 게이트웨이와 HTTP/2 협상 시 응답이 멈추는 경우가 있어 HTTP/1.1로 고정한다
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(properties.publicApi().connectTimeout())
                .build();
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(properties.publicApi().readTimeout());
        return builder.requestFactory(requestFactory).build();
    }
}
