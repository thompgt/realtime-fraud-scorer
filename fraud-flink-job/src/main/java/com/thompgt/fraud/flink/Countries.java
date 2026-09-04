package com.thompgt.fraud.flink;

import java.util.Map;

/**
 * Approximate country centroids, for the geo-impossibility rule.
 *
 * <p>A centroid is a crude stand-in for where a transaction actually happened — a US-to-CA
 * authorisation could be Seattle to Vancouver (230 km) or Miami to Vancouver (5000 km), and this
 * table answers about 4000 km for both. That is acceptable because the rule only asks whether the
 * implied speed is *physically impossible*, and it errs toward over-estimating distance, i.e.
 * toward false positives, which the score weighting and the Phase 6 retune can then correct. A real
 * system would use the acquirer's city or the terminal's coordinates.
 *
 * <p>Unknown country codes make the rule abstain rather than guess.
 */
final class Countries {

    private record Point(double lat, double lon) {
    }

    private static final double EARTH_RADIUS_KM = 6371.0;

    private static final Map<String, Point> CENTROIDS = Map.ofEntries(
            Map.entry("US", new Point(39.8, -98.6)),
            Map.entry("CA", new Point(56.1, -106.3)),
            Map.entry("MX", new Point(23.6, -102.6)),
            Map.entry("BR", new Point(-14.2, -51.9)),
            Map.entry("AR", new Point(-38.4, -63.6)),
            Map.entry("GB", new Point(55.4, -3.4)),
            Map.entry("IE", new Point(53.4, -8.2)),
            Map.entry("FR", new Point(46.2, 2.2)),
            Map.entry("DE", new Point(51.2, 10.5)),
            Map.entry("NL", new Point(52.1, 5.3)),
            Map.entry("BE", new Point(50.5, 4.5)),
            Map.entry("ES", new Point(40.5, -3.7)),
            Map.entry("PT", new Point(39.4, -8.2)),
            Map.entry("IT", new Point(41.9, 12.6)),
            Map.entry("CH", new Point(46.8, 8.2)),
            Map.entry("AT", new Point(47.5, 14.6)),
            Map.entry("PL", new Point(51.9, 19.1)),
            Map.entry("SE", new Point(60.1, 18.6)),
            Map.entry("NO", new Point(60.5, 8.5)),
            Map.entry("DK", new Point(56.3, 9.5)),
            Map.entry("FI", new Point(61.9, 25.7)),
            Map.entry("TR", new Point(38.9, 35.2)),
            Map.entry("RU", new Point(61.5, 105.3)),
            Map.entry("UA", new Point(48.4, 31.2)),
            Map.entry("ZA", new Point(-30.6, 22.9)),
            Map.entry("NG", new Point(9.1, 8.7)),
            Map.entry("KE", new Point(-0.0, 37.9)),
            Map.entry("EG", new Point(26.8, 30.8)),
            Map.entry("AE", new Point(23.4, 53.8)),
            Map.entry("SA", new Point(23.9, 45.1)),
            Map.entry("IN", new Point(20.6, 79.0)),
            Map.entry("PK", new Point(30.4, 69.3)),
            Map.entry("CN", new Point(35.9, 104.2)),
            Map.entry("JP", new Point(36.2, 138.3)),
            Map.entry("KR", new Point(35.9, 127.8)),
            Map.entry("SG", new Point(1.35, 103.8)),
            Map.entry("MY", new Point(4.2, 101.98)),
            Map.entry("TH", new Point(15.9, 101.0)),
            Map.entry("VN", new Point(14.1, 108.3)),
            Map.entry("ID", new Point(-0.8, 113.9)),
            Map.entry("PH", new Point(12.9, 121.8)),
            Map.entry("AU", new Point(-25.3, 133.8)),
            Map.entry("NZ", new Point(-40.9, 174.9)));

    private Countries() {
    }

    static boolean known(String code) {
        return code != null && CENTROIDS.containsKey(code);
    }

    /**
     * Great-circle distance between two country centroids in km, or -1 if either is unknown.
     *
     * <p>Haversine rather than the simpler equirectangular approximation: the latter degrades badly
     * at high latitude and across the antimeridian, and both cases occur in this data (Norway to
     * Japan, for instance).
     */
    static double distanceKm(String fromCode, String toCode) {
        Point from = CENTROIDS.get(fromCode);
        Point to = CENTROIDS.get(toCode);
        if (from == null || to == null) {
            return -1;
        }
        double dLat = Math.toRadians(to.lat() - from.lat());
        double dLon = Math.toRadians(to.lon() - from.lon());
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(from.lat())) * Math.cos(Math.toRadians(to.lat()))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return EARTH_RADIUS_KM * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}
