package org.n52.tsf.model.jts;

import org.apache.log4j.Logger;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.n52.tsf.serialization.protobuf.gen.GeoProtobuf;

import java.io.IOException;
import java.io.OutputStream;

/**
 * This class provides the JTS to ProtoBuf Serialization functionality.
 */
public class PBSerializationHandler {
    final static Logger logger = Logger.getLogger(PBSerializationHandler.class);

    public void serialize(Geometry jtsGeometry, OutputStream outputStream) throws IOException {
        if (jtsGeometry instanceof Point) {
            serializePoint((Point) jtsGeometry, outputStream);
        } else {
            logger.error("Unsupported Geometric type");
        }
    }

    public void serializePoint(Point jtsPoint, OutputStream outputStream) throws IOException {
        if (jtsPoint.getCoordinates().length == 0) {
            logger.error("No Coordinate data available");
        } else {
            GeoProtobuf.Coordinate.Builder cord = GeoProtobuf.Coordinate.newBuilder();
            cord.setX(jtsPoint.getX());
            cord.setY(jtsPoint.getY());
            cord.setZ(Double.NaN);
            GeoProtobuf.Geometry.Builder geo = GeoProtobuf.Geometry.newBuilder();
            geo.setType(GeoProtobuf.Geometry.Type.POINT);
            geo.addCoordinates(cord.build());
            geo.build().writeTo(outputStream);
        }
    }
}
