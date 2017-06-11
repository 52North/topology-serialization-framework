//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package org.n52.tsf.model.jts;

import org.apache.log4j.Logger;
import org.locationtech.jts.geom.*;
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
        } else if (jtsGeometry instanceof LineString) {
            serializeLineString((LineString) jtsGeometry, outputStream);
        } else if (jtsGeometry instanceof Polygon) {
            serializePolygon((Polygon) jtsGeometry, outputStream);
        } else if (jtsGeometry instanceof MultiPoint) {
            serializeMultiPoint((MultiPoint) jtsGeometry, outputStream);
        } else if (jtsGeometry instanceof MultiLineString) {
            serializeMultiLineString((MultiLineString) jtsGeometry, outputStream);
        } else {
            logger.error("Unsupported Geometric type");
        }
    }

    public void serializePoint(Point jtsPoint, OutputStream outputStream) throws IOException {
        if (jtsPoint.getCoordinates().length == 0) {
            logger.error("No Coordinate data available");
        } else {
            GeoProtobuf.Geometry.Builder geo = GeoProtobuf.Geometry.newBuilder();
            geo.setType(GeoProtobuf.Geometry.Type.POINT);
            geo.addCoordinates(createCoordinate(jtsPoint.getCoordinate()));
            geo.build().writeTo(outputStream);
        }
    }

    public void serializeLineString(LineString jtsLineString, OutputStream outputStream) throws IOException {
        if (jtsLineString.getCoordinates().length < 2) {
            logger.error("Insufficient Coordinates");
        } else {
            GeoProtobuf.Geometry.Builder geo = GeoProtobuf.Geometry.newBuilder();
            geo.setType(GeoProtobuf.Geometry.Type.LINESTRING);
            for (Coordinate coord : jtsLineString.getCoordinates()) {
                geo.addCoordinates(createCoordinate(coord));
            }
            geo.build().writeTo(outputStream);
        }
    }

    public void serializeMultiPoint(MultiPoint jtsMultiPoint, OutputStream outputStream) throws IOException {
        if (jtsMultiPoint.getCoordinates().length == 0) {
            logger.error("No Coordinate data available");
        } else {
            GeoProtobuf.Geometry.Builder geo = GeoProtobuf.Geometry.newBuilder();
            geo.setType(GeoProtobuf.Geometry.Type.MULTIPOINT);
            for (Coordinate coord : jtsMultiPoint.getCoordinates()) {
                geo.addCoordinates(createCoordinate(coord));
            }
            geo.build().writeTo(outputStream);
        }
    }

    public void serializeMultiLineString(MultiLineString jtsMultiLineString, OutputStream outputStream) throws IOException {
        GeoProtobuf.Geometry.Builder geo = GeoProtobuf.Geometry.newBuilder();
        geo.setType(GeoProtobuf.Geometry.Type.MULTILINESTRING);
        for (int i = 0; i < jtsMultiLineString.getNumGeometries(); i++) {
            Geometry jtsGeo = jtsMultiLineString.getGeometryN(i);
            GeoProtobuf.Geometry.Builder pbGeo = GeoProtobuf.Geometry.newBuilder();
            for (Coordinate coord : jtsGeo.getCoordinates()) {
                pbGeo.addCoordinates(createCoordinate(coord));
            }
            geo.addGeometries(pbGeo.build());
        }
        geo.build().writeTo(outputStream);
    }

    public void serializePolygon(Polygon jtsPolygon, OutputStream outputStream) throws IOException {
        LineString externalLS = jtsPolygon.getExteriorRing();
        int noOfInteriorRings = jtsPolygon.getNumInteriorRing();
        GeoProtobuf.Geometry.Builder geoPolygon = GeoProtobuf.Geometry.newBuilder();
        geoPolygon.setType(GeoProtobuf.Geometry.Type.POLYGON);

        GeoProtobuf.Geometry.Builder externalGeo = GeoProtobuf.Geometry.newBuilder();
        for (Coordinate coord : externalLS.getCoordinates()) {
            externalGeo.addCoordinates(createCoordinate(coord));
        }
        geoPolygon.addGeometries(externalGeo.build());

        if (noOfInteriorRings > 0) {
            GeoProtobuf.Geometry.Builder interiorGeos = GeoProtobuf.Geometry.newBuilder();
            for (int i = 0; i < noOfInteriorRings; i++) {
                LineString interiorLS = jtsPolygon.getInteriorRingN(i);
                GeoProtobuf.Geometry.Builder iGeo = GeoProtobuf.Geometry.newBuilder();
                for (Coordinate coord : interiorLS.getCoordinates()) {
                    iGeo.addCoordinates(createCoordinate(coord));
                }
                interiorGeos.addGeometries(iGeo.build());
            }
            geoPolygon.addGeometries(interiorGeos.build());
        }
        geoPolygon.build().writeTo(outputStream);
    }

    private GeoProtobuf.Coordinate createCoordinate(Coordinate jtsCoordinate) {
        GeoProtobuf.Coordinate.Builder coordinate = GeoProtobuf.Coordinate.newBuilder();
        coordinate.setX(jtsCoordinate.x);
        coordinate.setY(jtsCoordinate.y);
        coordinate.setZ(jtsCoordinate.z);
        return coordinate.build();
    }
}
