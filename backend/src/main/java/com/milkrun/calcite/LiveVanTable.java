package com.milkrun.calcite;

import com.milkrun.model.VanState;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * The live fleet as a Calcite table: one row per van, read from the ETA
 * engine's in-memory state at query time. Registered as {@code live.vans}
 * next to the PostgreSQL schema, so a single SQL statement can join what the
 * vans are doing right now with historical data in the database.
 */
public class LiveVanTable extends AbstractTable implements ScannableTable {

    private final Supplier<Collection<VanState>> vans;

    public LiveVanTable(Supplier<Collection<VanState>> vans) {
        this.vans = vans;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory f) {
        return f.builder()
                .add("van_id", SqlTypeName.VARCHAR)
                .add("route_id", SqlTypeName.VARCHAR)
                .add("status", SqlTypeName.VARCHAR)
                .add("lat", SqlTypeName.DOUBLE)
                .add("lon", SqlTypeName.DOUBLE)
                .add("speed_kmh", SqlTypeName.DOUBLE)
                .add("battery_pct", SqlTypeName.INTEGER)
                .add("current_stop", SqlTypeName.INTEGER)
                .add("total_stops", SqlTypeName.INTEGER)
                .add("eta_seconds", SqlTypeName.BIGINT)
                .add("sla_risk", SqlTypeName.VARCHAR)
                .add("slack_seconds", SqlTypeName.BIGINT).nullable(true)
                .add("in_zone", SqlTypeName.BOOLEAN)
                .add("zone_name", SqlTypeName.VARCHAR).nullable(true)
                .add("updated_epoch_ms", SqlTypeName.BIGINT)
                .build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return Linq4j.asEnumerable(vans.get().stream().map(LiveVanTable::row).toList());
    }

    private static Object[] row(VanState v) {
        return new Object[] {
                v.vanId(),
                v.routeId(),
                v.status() != null ? v.status().name() : null,
                v.location() != null ? v.location().latitude() : null,
                v.location() != null ? v.location().longitude() : null,
                v.speedKmh(),
                v.batteryPct(),
                v.currentStopIndex(),
                v.totalStops(),
                v.etaNextStopSeconds(),
                v.slaRisk() != null ? v.slaRisk().name() : null,
                v.slaSlackSeconds(),
                v.inGeofence(),
                v.geofenceName(),
                v.lastUpdated() != null ? v.lastUpdated().toEpochMilli() : null,
        };
    }
}
