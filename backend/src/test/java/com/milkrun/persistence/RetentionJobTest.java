package com.milkrun.persistence;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.core.DatabaseClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Retention against a real PostGIS database with the Flyway schema: old rows go,
 * recent ones stay, and a route's deliveries are deleted with it.
 */
@Testcontainers(disabledWithoutDocker = true)
class RetentionJobTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(
            DockerImageName.parse("postgis/postgis:16-3.4").asCompatibleSubstituteFor("postgres"));

    static DatabaseClient db;

    @BeforeAll
    static void migrate() {
        Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .baselineOnMigrate(true).baselineVersion("0").load().migrate();
        ConnectionFactory factory = ConnectionFactories.get("r2dbc:postgresql://%s:%s@%s:%d/%s".formatted(
                POSTGRES.getUsername(), POSTGRES.getPassword(), POSTGRES.getHost(),
                POSTGRES.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT), POSTGRES.getDatabaseName()));
        db = DatabaseClient.create(factory);
    }

    @BeforeEach
    void seed() throws SQLException {
        sql("TRUNCATE gps_archive, dead_letter_log, sla_breaches, delivery_logs, completed_routes");
        // 7 GPS points 2 days old, then 3 from the last hour (ids rise with age, as in production)
        sql("""
                INSERT INTO gps_archive (event_id, van_id, route_id, location, device_timestamp,
                                         ingestion_timestamp, created_at)
                SELECT gen_random_uuid(), 'van-1', 'r-old', ST_SetSRID(ST_MakePoint(4.9, 52.37), 4326),
                       t, t, t
                FROM (SELECT now() - INTERVAL '2 days' + n * INTERVAL '1 second' AS t
                      FROM generate_series(1, 7) n
                      UNION ALL
                      SELECT now() - INTERVAL '1 hour' + n * INTERVAL '1 second'
                      FROM generate_series(1, 3) n) times
                ORDER BY t
                """);
        sql("""
                INSERT INTO dead_letter_log (original_topic, van_id, event_payload, error_reason, created_at)
                VALUES ('gps-events', 'van-1', '{}', 'LATE', now() - INTERVAL '3 days'),
                       ('gps-events', 'van-1', '{}', 'LATE', now() - INTERVAL '1 hour')
                """);
        sql("""
                INSERT INTO completed_routes (route_id, van_id, planned_start, planned_end, total_stops, created_at)
                VALUES ('r-old', 'van-1', now(), now(), 2, now() - INTERVAL '8 days'),
                       ('r-new', 'van-1', now(), now(), 1, now() - INTERVAL '1 day')
                """);
        sql("""
                INSERT INTO delivery_logs (route_id, van_id, stop_index, customer_id, sla_deadline, delivery_status)
                VALUES ('r-old', 'van-1', 0, 'c', now(), 'DELIVERED'),
                       ('r-old', 'van-1', 1, 'c', now(), 'DELIVERED'),
                       ('r-new', 'van-1', 0, 'c', now(), 'DELIVERED')
                """);
        sql("""
                INSERT INTO sla_breaches (route_id, van_id, stop_index, customer_id, sla_deadline,
                                          predicted_arrival, breach_seconds, severity, detected_at)
                VALUES ('r-old', 'van-1', 0, 'c', now(), now(), 30, 'WARNING', now() - INTERVAL '8 days'),
                       ('r-new', 'van-1', 0, 'c', now(), now(), 30, 'WARNING', now() - INTERVAL '1 day')
                """);
    }

    private static RetentionJob job(int batchSize, int maxBatches) {
        return new RetentionJob(db, new SimpleMeterRegistry(), Duration.ofMinutes(10), batchSize, maxBatches,
                Duration.ofMillis(10), Duration.ofHours(24), Duration.ofDays(2), Duration.ofDays(7));
    }

    @Test
    void deletesRowsPastTheirRetentionAndKeepsRecentOnes() throws SQLException {
        long deleted = job(5000, 20).runOnce().block(Duration.ofSeconds(30));

        assertEquals(3, count("gps_archive"));
        assertEquals(1, count("dead_letter_log"));
        assertEquals(1, count("sla_breaches"));
        assertEquals(1, count("completed_routes"), "the 8-day-old route goes");
        assertEquals(1, count("delivery_logs"), "with its deliveries");
        assertEquals(7 + 1 + 1 + 1, deleted);
    }

    @Test
    void drainsABacklogInBatchesUpToTheLimitPerRun() throws SQLException {
        // Batches of 2: the 7 old GPS points need 4 batches; with 3 per run, one is left.
        job(2, 3).runOnce().block(Duration.ofSeconds(30));
        assertEquals(1 + 3, count("gps_archive"));

        job(2, 3).runOnce().block(Duration.ofSeconds(30));
        assertEquals(3, count("gps_archive"));
    }

    private static void sql(String statement) throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute(statement);
        }
    }

    private static long count(String table) throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT count(*) FROM " + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static Connection connect() throws SQLException {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }
}
