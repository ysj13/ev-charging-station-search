package com.evalarm.charger.station.repository;

import com.evalarm.charger.station.domain.Station;
import org.springframework.data.jpa.repository.JpaRepository;

public interface StationRepository extends JpaRepository<Station, String> {
}
