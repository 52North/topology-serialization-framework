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
        GeoProtobuf.Geometry pbGeometry;
        if (jtsGeometry instanceof Point) {
            pbGeometry = serializePoint((Point) jtsGeometry);
        } else if (jtsGeometry instanceof LineString) {
            pbGeometry = serializeLineString((LineString) jtsGeometry);
        } else if (jtsGeometry instanceof Polygon) {
            pbGeometry = serializePolygon((Polygon) jtsGeometry);
        } else if (jtsGeometry instanceof MultiPoint) {
            pbGeometry = serializeMultiPoint((MultiPoint) jtsGeometry);
        } else if (jtsGeometry instanceof MultiLineString) {
            pbGeometry = serializeMultiLineString((MultiLineString) jtsGeometry);
        } else {
            throw new IllegalArgumentException("Unsupported Geometric type");
        }
        pbGeometry.writeTo(outputStream);
    }

    private GeoProtobuf.Geometry serializePoint(Point jtsPoint) throws IOException {
        if (jtsPoint.getCoordinates().length == 0) {
            throw new IllegalArgumentException("No Coordinate data available");
        } else {
            GeoProtobuf.Geometry.Builder geoPoint = GeoProtobuf.Geometry.newBuilder();
            geoPoint.setType(GeoProtobuf.Geometry.Type.POINT);
            geoPoint.addCoordinates(createCoordinate(jtsPoint.getCoordinate()));
            return  geoPoint.build();
        }
    }

    private GeoProtobuf.Geometry serializeLineString(LineString jtsLineString) throws IOException {
        if (jtsLineString.getCoordinates().length < 2) {
            throw new IllegalArgumentException("Insufficient Coordinates");
        } else {
            GeoProtobuf.Geometry.Builder geoLineString = GeoProtobuf.Geometry.newBuilder();
            geoLineString.setType(GeoProtobuf.Geometry.Type.LINESTRING);
            for (Coordinate coord : jtsLineString.getCoordinates()) {
                geoLineString.addCoordinates(createCoordinate(coord));
            }
            return geoLineString.build();
        }
    }

    private GeoProtobuf.Geometry serializeMultiPoint(MultiPoint jtsMultiPoint) throws IOException {
        GeoProtobuf.Geometry.Builder geoMultiPoint = GeoProtobuf.Geometry.newBuilder();
        geoMultiPoint.setType(GeoProtobuf.Geometry.Type.MULTIPOINT);
        for (int i = 0; i < jtsMultiPoint.getNumGeometries(); i++) {
            geoMultiPoint.addGeometries(serializePoint((Point) jtsMultiPoint.getGeometryN(i)));
        }
        return geoMultiPoint.build();
    }

    private GeoProtobuf.Geometry serializeMultiLineString(MultiLineString jtsMultiLineString) throws IOException {
        GeoProtobuf.Geometry.Builder geoMultiLineString = GeoProtobuf.Geometry.newBuilder();
        geoMultiLineString.setType(GeoProtobuf.Geometry.Type.MULTILINESTRING);
        for (int i = 0; i < jtsMultiLineString.getNumGeometries(); i++) {
            geoMultiLineString.addGeometries(serializeLineString((LineString) jtsMultiLineString.getGeometryN(i)));
        }
        return geoMultiLineString.build();
    }

    private GeoProtobuf.Geometry serializePolygon(Polygon jtsPolygon) throws IOException {
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
        return geoPolygon.build();
    }

    private GeoProtobuf.Coordinate createCoordinate(Coordinate jtsCoordinate) {
        GeoProtobuf.Coordinate.Builder coordinate = GeoProtobuf.Coordinate.newBuilder();
        coordinate.setX(jtsCoordinate.x);
        coordinate.setY(jtsCoordinate.y);
        coordinate.setZ(jtsCoordinate.z);
        return coordinate.build();
    }
}
