package com.milkrun.persistence;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** V6 on a database holding old and recent history: what it keeps and what it drops. */
@Testcontainers(disabledWithoutDocker = true)
class HistoryCompactionMigrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(
            DockerImageName.parse("postgis/postgis:16-3.4").asCompatibleSubstituteFor("postgres"));

    static Flyway flyway(String target) {
        return Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .baselineOnMigrate(true).baselineVersion("0").target(target).load();
    }

    @BeforeAll
    static void migrateWithHistory() throws SQLException {
        flyway("5").migrate();
        sql("""
                INSERT INTO completed_routes (route_id, van_id, planned_start, planned_end, total_stops,
                                              completed_stops, status, created_at)
                VALUES ('r-old', 'van-1', now(), now(), 2, 2, 'COMPLETED', now() - INTERVAL '8 days'),
                       ('r-new', 'van-1', now(), now(), 2, 2, 'COMPLETED', now() - INTERVAL '1 day')
                """);
        sql("""
                INSERT INTO delivery_logs (route_id, van_id, stop_index, customer_id, sla_deadline, delivery_status)
                VALUES ('r-old', 'van-1', 0, 'c', now(), 'DELIVERED'),
                       ('r-new', 'van-1', 0, 'c', now(), 'DELIVERED')
                """);
        sql("""
                INSERT INTO sla_breaches (route_id, van_id, stop_index, customer_id, sla_deadline,
                                          predicted_arrival, breach_seconds, severity, detected_at, geofence_id)
                VALUES ('r-old', 'van-1', 0, 'c', now(), now(), 30, 'WARNING', now() - INTERVAL '8 days', NULL),
                       ('r-new', 'van-1', 0, 'c', now(), now(), 90, 'CRITICAL', now() - INTERVAL '1 day',
                        (SELECT id FROM geofence_zones ORDER BY name LIMIT 1))
                """);
        flyway("latest").migrate();
    }

    @Test
    void keepsTheLastSevenDaysOfBreachesAndRoutesWithTheirValues() throws SQLException {
        assertEquals("r-new", single("SELECT string_agg(route_id, ',') FROM sla_breaches"));
        assertEquals("90 CRITICAL true", single("""
                SELECT breach_seconds || ' ' || severity || ' ' || (geofence_id IS NOT NULL) FROM sla_breaches"""));
        assertEquals("r-new", single("SELECT string_agg(route_id, ',') FROM completed_routes"));
        assertEquals("2", single("SELECT completed_stops::text FROM completed_routes"));
    }

    @Test
    void dropsTheDeliveryLog() throws SQLException {
        assertEquals("0", single("SELECT count(*)::text FROM delivery_logs"));
    }

    @Test
    void indexesRouteCreationTimeAndKeepsTheConstraints() throws SQLException {
        assertEquals("1", single("SELECT count(*)::text FROM pg_indexes WHERE indexname = 'idx_routes_created'"));
        // The foreign key from delivery_logs to completed_routes still holds
        SQLException e = org.junit.jupiter.api.Assertions.assertThrows(SQLException.class, () -> sql("""
                INSERT INTO delivery_logs (route_id, van_id, stop_index, customer_id, sla_deadline, delivery_status)
                VALUES ('no-such-route', 'van-1', 0, 'c', now(), 'DELIVERED')
                """));
        assertTrue(e.getMessage().contains("foreign key"), e.getMessage());
    }

    private static void sql(String statement) throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute(statement);
        }
    }

    private static String single(String query) throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(query)) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static Connection connect() throws SQLException {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }
}
