package com.milkrun.engine;

/**
 * A route polyline prepared for ETA work: cumulative distance at every vertex
 * and the delay-zone speed factor of every segment.
 */
public final class RouteGeometry {

    private static final double EARTH_RADIUS_M = 6_371_000;
    private static final double METERS_PER_DEGREE = Math.PI / 180 * EARTH_RADIUS_M;

    private final double[] lat;
    private final double[] lon;
    private final double[] cum;
    private final double[] segmentFactor;
    private final int zonesVersion;

    private RouteGeometry(double[] lat, double[] lon, double[] cum, double[] segmentFactor, int zonesVersion) {
        this.lat = lat;
        this.lon = lon;
        this.cum = cum;
        this.segmentFactor = segmentFactor;
        this.zonesVersion = zonesVersion;
    }

    /**
     * @param waypoints [latitude, longitude] pairs
     * @param geofence  speed factor of each segment is taken at its midpoint
     */
    public static RouteGeometry of(double[][] waypoints, GeofenceDetector geofence) {
        int n = waypoints.length;
        double[] lat = new double[n];
        double[] lon = new double[n];
        double[] cum = new double[n];
        for (int i = 0; i < n; i++) {
            lat[i] = waypoints[i][0];
            lon[i] = waypoints[i][1];
            if (i > 0) {
                cum[i] = cum[i - 1] + haversineMeters(lat[i - 1], lon[i - 1], lat[i], lon[i]);
            }
        }
        double[] factor = new double[Math.max(0, n - 1)];
        for (int i = 0; i < n - 1; i++) {
            factor[i] = geofence.speedFactorAt((lat[i] + lat[i + 1]) / 2, (lon[i] + lon[i + 1]) / 2);
        }
        return new RouteGeometry(lat, lon, cum, factor, geofence.version());
    }

    public int vertexCount() {
        return lat.length;
    }

    public double length() {
        return cum[cum.length - 1];
    }

    public double distanceAt(int vertex) {
        return cum[vertex];
    }

    /** Geofence version the segment factors were computed with. */
    public int zonesVersion() {
        return zonesVersion;
    }

    /** Where a position lies along the route. */
    public record Match(int segment, double distanceAlong, double offRouteMeters) {
    }

    /**
     * Projects a position onto the part of the route between two vertices (the
     * current leg), returning the closest point.
     */
    public Match locate(double pLat, double pLon, int fromVertex, int toVertex) {
        int last = lat.length - 1;
        fromVertex = clamp(fromVertex, 0, last);
        toVertex = clamp(toVertex, fromVertex, last);
        if (fromVertex == toVertex) {
            int seg = Math.min(fromVertex, last - 1);
            return new Match(seg, cum[fromVertex], haversineMeters(pLat, pLon, lat[fromVertex], lon[fromVertex]));
        }
        double cosLat = Math.cos(Math.toRadians(pLat));
        Match best = null;
        for (int i = fromVertex; i < toVertex; i++) {
            // Local planar coordinates in metres, centred on the position
            double ax = (lon[i] - pLon) * cosLat * METERS_PER_DEGREE;
            double ay = (lat[i] - pLat) * METERS_PER_DEGREE;
            double bx = (lon[i + 1] - pLon) * cosLat * METERS_PER_DEGREE;
            double by = (lat[i + 1] - pLat) * METERS_PER_DEGREE;
            double dx = bx - ax;
            double dy = by - ay;
            double len2 = dx * dx + dy * dy;
            double t = len2 > 0 ? clamp(-(ax * dx + ay * dy) / len2, 0, 1) : 0;
            double px = ax + t * dx;
            double py = ay + t * dy;
            double distance = Math.sqrt(px * px + py * py);
            if (best == null || distance < best.offRouteMeters()) {
                best = new Match(i, cum[i] + t * (cum[i + 1] - cum[i]), distance);
            }
        }
        return best;
    }

    /**
     * Seconds to drive from {@code fromDistance} to vertex {@code toVertex} at
     * a free-flow speed, slowed by each segment's zone factor.
     */
    public double travelSeconds(double fromDistance, int toVertex, double freeFlowMps) {
        double target = cum[clamp(toVertex, 0, cum.length - 1)];
        if (fromDistance >= target || freeFlowMps <= 0) {
            return 0;
        }
        double seconds = 0;
        for (int i = segmentAt(fromDistance); i < segmentFactor.length && cum[i] < target; i++) {
            double start = Math.max(cum[i], fromDistance);
            double end = Math.min(cum[i + 1], target);
            if (end > start) {
                seconds += (end - start) / (freeFlowMps * segmentFactor[i]);
            }
        }
        return seconds;
    }

    /** Zone speed factor at a distance along the route. */
    public double factorAt(double distance) {
        return segmentFactor.length == 0 ? 1.0 : segmentFactor[segmentAt(distance)];
    }

    private int segmentAt(double distance) {
        int lo = 0;
        int hi = segmentFactor.length - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (cum[mid] <= distance) lo = mid;
            else hi = mid - 1;
        }
        return Math.max(0, lo);
    }

    private static int clamp(int v, int lo, int hi) {
        return Math.max(lo, Math.min(hi, v));
    }

    private static double clamp(double v, double lo, double hi) {
        return Math.max(lo, Math.min(hi, v));
    }

    public static double haversineMeters(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double h = Math.pow(Math.sin(dLat / 2), 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.pow(Math.sin(dLon / 2), 2);
        return 2 * EARTH_RADIUS_M * Math.asin(Math.min(1, Math.sqrt(h)));
    }
}
