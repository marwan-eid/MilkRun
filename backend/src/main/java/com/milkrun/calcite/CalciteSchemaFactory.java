package com.milkrun.calcite;

import com.milkrun.fleet.FleetView;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.annotation.PreDestroy;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.time.Duration;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Apache Calcite federation over two sources:
 *
 * <ul>
 *   <li>{@code milkrun}: the PostgreSQL database through Calcite's JDBC
 *       adapter (filters, joins and aggregates are pushed down to Postgres);</li>
 *   <li>{@code live}: the fleet's current state held in memory by the ETA
 *       engine ({@link LiveVanTable}).</li>
 * </ul>
 *
 * One SQL statement can join both. Queries run over JDBC, so callers must
 * keep them off the reactive event loop (see {@link AnalyticsService}).
 * The connection is opened lazily and retried on the next query if the
 * database was not reachable.
 */
@Component
public class CalciteSchemaFactory {

    private static final Logger log = LoggerFactory.getLogger(CalciteSchemaFactory.class);

    private final DataSource postgres;
    private final LiveVanTable liveVans;
    private final boolean ownsDataSource;
    private volatile Connection connection;

    @Autowired
    public CalciteSchemaFactory(
            FleetView fleet,
            @Value("${spring.r2dbc.url}") String r2dbcUrl,
            @Value("${spring.r2dbc.username}") String username,
            @Value("${spring.r2dbc.password}") String password) {
        this(pooledDataSource(toJdbcUrl(r2dbcUrl), username, password),
                new LiveVanTable(fleet::all), true);
    }

    /** For tests: any data source and live table. */
    public CalciteSchemaFactory(DataSource postgres, LiveVanTable liveVans) {
        this(postgres, liveVans, false);
    }

    private CalciteSchemaFactory(DataSource postgres, LiveVanTable liveVans, boolean ownsDataSource) {
        this.postgres = postgres;
        this.liveVans = liveVans;
        this.ownsDataSource = ownsDataSource;
    }

    static String toJdbcUrl(String r2dbcUrl) {
        return r2dbcUrl
                .replace("r2dbc:pool:postgresql", "jdbc:postgresql")
                .replace("r2dbc:postgresql", "jdbc:postgresql");
    }

    private static DataSource pooledDataSource(String jdbcUrl, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setPoolName("calcite-postgres");
        config.setMaximumPoolSize(3);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(5_000);
        // Don't fail at startup if the database is still coming up
        config.setInitializationFailTimeout(-1);
        return new HikariDataSource(config);
    }

    /** Opens the Calcite connection on first use; throws if the database is unreachable. */
    synchronized Connection connection() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            return connection;
        }
        Properties info = new Properties();
        info.setProperty("lex", "JAVA"); // case-sensitive, unquoted identifiers
        Connection c = DriverManager.getConnection("jdbc:calcite:", info);
        try {
            CalciteConnection calcite = c.unwrap(CalciteConnection.class);
            SchemaPlus root = calcite.getRootSchema();
            SchemaPlus milkrun = root.add("milkrun", JdbcSchema.create(root, "milkrun", postgres, null, "public"));
            root.add("live", new AbstractSchema() {
                @Override
                protected Map<String, Table> getTableMap() {
                    return Map.of("vans", liveVans);
                }
            });
            // Fail now rather than on the first query if Postgres is unreachable
            log.info("Calcite federation ready: milkrun tables {}, live tables [vans]", milkrun.getTableNames());
        } catch (Exception e) {
            c.close();
            throw e instanceof SQLException sql ? sql : new SQLException("Calcite initialisation failed", e);
        }
        connection = c;
        return c;
    }

    /**
     * Runs a query and returns rows as column-name to value maps.
     *
     * @throws SQLException on any failure; callers decide how to report it
     */
    public List<Map<String, Object>> executeQuery(String sql, Object... params) throws SQLException {
        try (PreparedStatement stmt = connection().prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                stmt.setObject(i + 1, params[i]);
            }
            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                List<Map<String, Object>> rows = new ArrayList<>();
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        row.put(meta.getColumnLabel(i).toLowerCase(), rs.getObject(i));
                    }
                    rows.add(row);
                }
                return rows;
            }
        } catch (SQLException e) {
            // A broken connection is reopened on the next query
            if (connection != null && !isValid(connection)) {
                closeQuietly();
            }
            throw e;
        }
    }

    /** Calcite's plan for a query (used to check what is pushed down to Postgres). */
    public String explain(String sql) throws SQLException {
        try (PreparedStatement stmt = connection().prepareStatement("EXPLAIN PLAN FOR " + sql);
                ResultSet rs = stmt.executeQuery()) {
            StringBuilder plan = new StringBuilder();
            while (rs.next()) {
                plan.append(rs.getString(1));
            }
            return plan.toString();
        }
    }

    /** Opens the connection in the background after startup, retrying until Postgres answers. */
    @EventListener(ApplicationReadyEvent.class)
    public void warmUp() {
        Mono.fromCallable(this::connection)
                .subscribeOn(Schedulers.boundedElastic())
                .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(10))
                        .doBeforeRetry(s -> log.warn("Calcite not ready yet: {}", s.failure().getMessage())))
                .subscribe();
    }

    public boolean isReady() {
        return connection != null;
    }

    private static boolean isValid(Connection c) {
        try {
            return !c.isClosed();
        } catch (SQLException e) {
            return false;
        }
    }

    private synchronized void closeQuietly() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException ignored) {
            // already broken
        }
        connection = null;
    }

    @PreDestroy
    public void shutdown() {
        closeQuietly();
        if (ownsDataSource && postgres instanceof HikariDataSource hikari) {
            hikari.close();
        }
    }
}
