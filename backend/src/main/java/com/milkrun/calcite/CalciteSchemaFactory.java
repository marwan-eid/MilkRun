package com.milkrun.calcite;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.sql.*;
import java.util.*;

/**
 * Apache Calcite schema factory that creates a federated query layer
 * over the PostgreSQL milkrun database.
 *
 * Calcite acts as a query parser and optimizer that sits on top of JDBC.
 * This allows us to:
 * 1. Run complex analytical SQL without overloading the R2DBC connection pool
 * 2. Demonstrate query federation (combining multiple data sources)
 * 3. Leverage Calcite's cost-based optimizer for analytical queries
 *
 * In a production system, you could federate across PostgreSQL + Elasticsearch
 * or PostgreSQL + in-memory data structures.
 */
@Component
public class CalciteSchemaFactory {

    private static final Logger log = LoggerFactory.getLogger(CalciteSchemaFactory.class);

    @Value("${spring.r2dbc.url}")
    private String r2dbcUrl;

    @Value("${spring.r2dbc.username}")
    private String username;

    @Value("${spring.r2dbc.password}")
    private String password;

    private Connection calciteConnection;

    @PostConstruct
    public void initialize() {
        try {
            // Convert R2DBC URL to JDBC URL
            String jdbcUrl = r2dbcUrl
                    .replace("r2dbc:postgresql", "jdbc:postgresql")
                    .replace("r2dbc:pool:postgresql", "jdbc:postgresql");

            // Create Calcite connection with embedded PostgreSQL schema
            Properties info = new Properties();
            info.setProperty("lex", "JAVA"); // Use Java-style identifier quoting

            calciteConnection = DriverManager.getConnection("jdbc:calcite:", info);
            CalciteConnection cc = calciteConnection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = cc.getRootSchema();

            // Create a JDBC-backed schema pointing at our PostgreSQL database
            DataSource pgDataSource = createDataSource(jdbcUrl);
            SchemaPlus milkrunSchema = rootSchema.add("milkrun",
                    JdbcSchema.create(rootSchema, "milkrun", pgDataSource, null, "public"));

            cc.setSchema("milkrun");

            log.info("✅ Calcite schema initialized with PostgreSQL federation");
            log.info("   Tables available: {}", milkrunSchema.getTableNames());

        } catch (Exception e) {
            log.warn("⚠️ Calcite initialization failed (DB may not be ready): {}", e.getMessage());
        }
    }

    @PreDestroy
    public void shutdown() {
        try {
            if (calciteConnection != null && !calciteConnection.isClosed()) {
                calciteConnection.close();
            }
        } catch (SQLException e) {
            log.warn("Error closing Calcite connection: {}", e.getMessage());
        }
    }

    /**
     * Execute a Calcite SQL query and return results as a list of maps.
     */
    public List<Map<String, Object>> executeQuery(String sql) {
        List<Map<String, Object>> results = new ArrayList<>();

        if (calciteConnection == null) {
            log.warn("Calcite not initialized, returning empty results");
            return results;
        }

        try (Statement stmt = calciteConnection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(meta.getColumnLabel(i).toLowerCase(), rs.getObject(i));
                }
                results.add(row);
            }

        } catch (SQLException e) {
            String cause = e.getCause() != null ? e.getCause().getMessage() : "Unknown";
            log.error("Calcite query failed: {} — Cause: {} — SQL: {}", e.getMessage(), cause, sql);
        }

        return results;
    }

    private DataSource createDataSource(String jdbcUrl) {
        org.postgresql.ds.PGSimpleDataSource ds = new org.postgresql.ds.PGSimpleDataSource();
        ds.setUrl(jdbcUrl);
        ds.setUser(username);
        ds.setPassword(password);
        return ds;
    }

    public boolean isReady() {
        return calciteConnection != null;
    }
}
