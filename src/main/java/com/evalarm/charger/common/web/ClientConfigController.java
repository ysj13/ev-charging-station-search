package com.evalarm.charger.common.web;

import com.evalarm.charger.common.config.AppProperties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** 검색 화면이 시작할 때 읽는 설정. 키를 정적 파일에 넣지 않고 환경 변수에서 주입하기 위해 둔다 */
@RestController
public class ClientConfigController {

    /**
     * @param kakaoMapJsKey 비어 있으면 화면은 지도 없이 목록만 보여 준다
     * @param devToolsEnabled 개발 도구(/dev 경로)가 켜져 있으면 화면에 테스트 버튼을 보여 준다
     */
    public record ClientConfig(String kakaoMapJsKey, boolean devToolsEnabled) {
    }

    private final AppProperties properties;

    /** 개발 도구 엔드포인트가 실제로 등록됐는지로 판단한다(프로퍼티 값 표기와 무관하게 엔드포인트 존재와 일치) */
    private final boolean devToolsEnabled;

    public ClientConfigController(AppProperties properties, ObjectProvider<DevToolsEndpoint> devTools) {
        this.properties = properties;
        this.devToolsEnabled = devTools.getIfAvailable() != null;
    }

    @GetMapping("/api/client-config")
    public ResponseEntity<ClientConfig> clientConfig() {
        return ResponseEntity.ok()
                // 설정이 바뀌면(재시작) 바로 반영되도록 매번 서버에 확인한다. 화면 시작 때 한 번 읽는 작은 응답이다
                .cacheControl(CacheControl.noCache())
                .body(new ClientConfig(properties.web().kakaoMapJsKey(), devToolsEnabled));
    }
}
