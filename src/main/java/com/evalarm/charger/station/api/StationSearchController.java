package com.evalarm.charger.station.api;

import com.evalarm.charger.station.query.StationDetail;
import com.evalarm.charger.station.query.StationQueryRepository;
import com.evalarm.charger.station.query.StationSearchResult;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/stations")
public class StationSearchController {

    private final StationQueryRepository queryRepository;

    public StationSearchController(StationQueryRepository queryRepository) {
        this.queryRepository = queryRepository;
    }

    /**
     * 충전소 검색.
     * 예) /api/stations?lat=37.4979&lng=127.0276&radius=1000&availableOnly=true
     *     /api/stations?q=강남구청&minOutputKw=50
     *     /api/stations?zscode=11680&page=0&size=20
     */
    @GetMapping
    public StationSearchResult search(@Valid @ModelAttribute StationSearchRequest request) {
        return queryRepository.search(request.toCriteria());
    }

    /** 충전소 상세와 충전기별 현재 상태 */
    @GetMapping("/{stationId}")
    public StationDetail detail(@PathVariable String stationId) {
        StationDetail detail = queryRepository.findDetail(stationId);
        if (detail == null) {
            throw new StationNotFoundException(stationId);
        }
        return detail;
    }
}
