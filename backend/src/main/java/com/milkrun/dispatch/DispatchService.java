package com.milkrun.dispatch;

import com.milkrun.engine.EtaEngine;
import com.milkrun.fleet.FleetView;
import com.milkrun.engine.RouteGeometry;
import com.milkrun.engine.RoutePlanStore;
import com.milkrun.model.Location;
import com.milkrun.model.RoutePlan;
import com.milkrun.model.VanState;
import com.milkrun.model.VanStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Cheapest-insertion dispatch with SLA checks.
 *
 * For every van still out on its route, the order is tried before each stop
 * the van has not reached yet (and after its last stop). An insertion costs
 * the extra driving distance, d(prev, order) + d(order, next) - d(prev, next),
 * plus a penalty for every stop it would push past its deadline and for
 * missing the order's own slot. The cheapest insertion across the fleet wins.
 *
 * Distances are straight-line times a detour factor (road geometry for the
 * detour is only fetched by the van that gets the order). Arrival times start
 * from the ETA engine's current prediction for the van and assume its current
 * delay carries over to later stops.
 */
@Service
public class DispatchService implements DispatchPlanner {

    private final RoutePlanStore plans;
    private final EtaEngine etaEngine;
    private final FleetView fleet;
    private final Clock clock;
    private final double detourFactor;
    private final double slotSimSeconds;
    private final double dwellSimSeconds;
    private final double latePenaltyKm;

    @Autowired
    public DispatchService(RoutePlanStore plans, EtaEngine etaEngine, FleetView fleet,
            @Value("${milkrun.eta.detour-factor:1.3}") double detourFactor,
            @Value("${milkrun.dispatch.slot-sim-seconds:1200}") double slotSimSeconds,
            @Value("${milkrun.dispatch.dwell-sim-seconds:40}") double dwellSimSeconds,
            @Value("${milkrun.dispatch.late-penalty-km:5}") double latePenaltyKm) {
        this(plans, etaEngine, fleet, Clock.systemUTC(), detourFactor, slotSimSeconds, dwellSimSeconds, latePenaltyKm);
    }

    DispatchService(RoutePlanStore plans, EtaEngine etaEngine, FleetView fleet, Clock clock, double detourFactor,
            double slotSimSeconds, double dwellSimSeconds, double latePenaltyKm) {
        this.plans = plans;
        this.etaEngine = etaEngine;
        this.fleet = fleet;
        this.clock = clock;
        this.detourFactor = detourFactor;
        this.slotSimSeconds = slotSimSeconds;
        this.dwellSimSeconds = dwellSimSeconds;
        this.latePenaltyKm = latePenaltyKm;
    }

    private record Option(VanState van, RoutePlan plan, int insertBefore, double extraMeters, Instant arrival,
            Instant deadline, int stopsMadeLate, boolean onTime, double cost) {
    }

    @Override
    public Optional<Assignment> assign(Order order) {
        Instant now = clock.instant();
        Option best = null;
        int considered = 0;

        for (VanState van : fleet.all()) {
            if (van.status() != VanStatus.EN_ROUTE && van.status() != VanStatus.DELIVERING) {
                continue;
            }
            Optional<RoutePlanStore.PlannedRoute> planned = plans.get(van.vanId(), van.routeId());
            if (planned.isEmpty()) {
                continue;
            }
            RoutePlan plan = planned.get().plan();
            List<RoutePlan.Stop> stops = plan.stops();
            boolean delivering = van.status() == VanStatus.DELIVERING;
            int next = delivering ? van.currentStopIndex() + 1 : van.currentStopIndex();
            if (next < 0 || next > stops.size() || (delivering && van.currentStopIndex() >= stops.size())) {
                continue;
            }

            double speed = etaEngine.freeFlowSpeed(van.vanId())
                    .orElse(plan.baseSpeedKmh() / 3.6 * plan.timeScale());
            Duration dwell = seconds(dwellSimSeconds / plan.timeScale());
            Instant deadline = order.requestedAt().plus(seconds(slotSimSeconds / plan.timeScale()));
            Duration delay = currentDelay(van, stops, next, delivering);
            double[] last = plan.waypoints()[plan.waypoints().length - 1];
            Location hub = new Location(last[0], last[1]);

            for (int k = next; k <= stops.size(); k++) {
                considered++;
                Location prev;
                Instant leavePrev;
                if (k == next && !delivering) {
                    prev = van.location();
                    leavePrev = now;
                } else if (k == next) {
                    prev = stops.get(van.currentStopIndex()).location();
                    leavePrev = now.plus(dwell);
                } else {
                    RoutePlan.Stop previous = stops.get(k - 1);
                    prev = previous.location();
                    Instant projectedPrev = projected(previous, delay);
                    leavePrev = (projectedPrev != null ? latest(now, projectedPrev) : now).plus(dwell);
                }
                Location nextLoc = k < stops.size() ? stops.get(k).location() : hub;

                double toOrder = road(prev, order.location());
                double extra = toOrder + road(order.location(), nextLoc) - road(prev, nextLoc);
                Instant arrival = leavePrev.plus(seconds(toOrder / speed));
                Duration added = seconds(Math.max(0, extra) / speed).plus(dwell);

                int madeLate = 0;
                for (int j = k; j < stops.size(); j++) {
                    RoutePlan.Stop s = stops.get(j);
                    Instant projected = projected(s, delay);
                    if (projected != null && !projected.isAfter(s.slaDeadline())
                            && projected.plus(added).isAfter(s.slaDeadline())) {
                        madeLate++;
                    }
                }
                boolean onTime = !arrival.isAfter(deadline);
                double cost = extra / 1000 + latePenaltyKm * madeLate + (onTime ? 0 : latePenaltyKm);

                Option option = new Option(van, plan, k, extra, arrival, deadline, madeLate, onTime, cost);
                if (best == null || option.cost() < best.cost()
                        || (option.cost() == best.cost() && option.arrival().isBefore(best.arrival()))) {
                    best = option;
                }
            }
        }

        if (best == null) {
            return Optional.empty();
        }
        return Optional.of(new Assignment(
                order.orderId(), best.van().vanId(), best.plan().routeId(),
                order.location().latitude(), order.location().longitude(),
                best.insertBefore(), best.plan().stops().size() + 1,
                Math.round(best.extraMeters() / 10.0) / 100.0,
                best.arrival(), best.deadline(), best.stopsMadeLate(), best.onTime(), considered, now));
    }

    /** How far the van runs behind its plan right now. */
    private static Duration currentDelay(VanState van, List<RoutePlan.Stop> stops, int next, boolean delivering) {
        if (van.predictedArrival() == null) {
            return Duration.ZERO;
        }
        int reference = delivering ? van.currentStopIndex() : next;
        if (reference < 0 || reference >= stops.size() || stops.get(reference).plannedArrival() == null) {
            return Duration.ZERO;
        }
        return Duration.between(stops.get(reference).plannedArrival(), van.predictedArrival());
    }

    /** When the van should now reach a stop, keeping its current delay; null if the plan has no time. */
    private static Instant projected(RoutePlan.Stop stop, Duration delay) {
        return stop.plannedArrival() != null ? stop.plannedArrival().plus(delay) : null;
    }

    private double road(Location a, Location b) {
        return RouteGeometry.haversineMeters(a.latitude(), a.longitude(), b.latitude(), b.longitude()) * detourFactor;
    }

    private static Duration seconds(double s) {
        return Duration.ofMillis(Math.round(s * 1000));
    }

    private static Instant latest(Instant a, Instant b) {
        return a.isAfter(b) ? a : b;
    }
}
