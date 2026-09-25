package com.milkrun.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.config.CorsRegistry;
import org.springframework.web.reactive.config.WebFluxConfigurer;

import java.util.List;

/**
 * Single place for CORS rules.
 *
 * The dashboard is served from the same origin as the API (nginx proxies /api),
 * so it needs no CORS at all. The allow-list exists for the Vite dev server and
 * for the personal site, which embeds the live stream read-only.
 */
@Configuration
public class CorsConfig implements WebFluxConfigurer {

    private final List<String> allowedOrigins;

    public CorsConfig(@Value("${milkrun.cors.allowed-origins}") List<String> allowedOrigins) {
        this.allowedOrigins = allowedOrigins;
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/api/**")
                .allowedOrigins(allowedOrigins.toArray(String[]::new))
                .allowedMethods("GET", "POST")
                .allowedHeaders("Content-Type")
                .maxAge(3600);
    }
}
