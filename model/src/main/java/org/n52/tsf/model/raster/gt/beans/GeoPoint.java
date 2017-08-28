package org.n52.tsf.model.raster.gt.beans;

/**
 * Represents grid to world geometric point
 */
public class GeoPoint {

    private double longitude;
    private double latitude;

    public GeoPoint(double longitude, double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public double getLatitude() {
        return latitude;
    }
}
