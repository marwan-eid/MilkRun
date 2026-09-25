package com.milkrun.fleet;

import com.milkrun.consumer.GpsEventPipeline;
import com.milkrun.engine.EtaEngine;
import com.milkrun.model.VanState;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.Collection;
import java.util.Optional;

/** Single-instance fleet view: this instance's own pipeline sees every van. */
@Component
@ConditionalOnProperty(name = "milkrun.fanout.mode", havingValue = "local", matchIfMissing = true)
public class LocalFleetView implements FleetView {

    private final EtaEngine etaEngine;
    private final GpsEventPipeline pipeline;

    public LocalFleetView(EtaEngine etaEngine, GpsEventPipeline pipeline) {
        this.etaEngine = etaEngine;
        this.pipeline = pipeline;
    }

    @Override
    public Collection<VanState> all() {
        return etaEngine.getAllVanStates().values();
    }

    @Override
    public Optional<VanState> get(String vanId) {
        return Optional.ofNullable(etaEngine.getAllVanStates().get(vanId));
    }

    @Override
    public Flux<VanState> updates() {
        return pipeline.rawVanStateStream();
    }
}
