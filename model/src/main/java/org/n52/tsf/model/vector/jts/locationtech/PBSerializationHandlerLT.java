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

package org.n52.tsf.model.vector.jts.locationtech;

import org.apache.log4j.Logger;
import org.locationtech.jts.geom.*;
import org.n52.tsf.model.SerializationHandler;
import org.n52.tsf.model.SerializerType;
import org.n52.tsf.serialization.protobuf.gen.GeoProtobuf;

import java.io.IOException;
import java.io.OutputStream;

/**
 * This class provides the JTS to ProtoBuf Serialization functionality.
 */
public class PBSerializationHandlerLT extends SerializationHandler {
    final static Logger logger = Logger.getLogger(PBSerializationHandlerLT.class);
    private OutputStream output;

    public PBSerializationHandlerLT(OutputStream outputStream) {
        super(SerializerType.PROTOBUF_SERIALIZER_VS);
        output = outputStream;
    }

    public void serialize(Object jtsGeometry) throws IOException {
        GeoProtobuf.Geometry pbGeometry;
        if (jtsGeometry instanceof Point) {
            pbGeometry = serializePoint((Point) jtsGeometry);
        }else if (jtsGeometry instanceof LinearRing) {
            pbGeometry = serializeLinearRing((LinearRing) jtsGeometry);
        } else if (jtsGeometry instanceof LineString) {
            pbGeometry = serializeLineString((LineString) jtsGeometry);
        } else if (jtsGeometry instanceof Polygon) {
            pbGeometry = serializePolygon((Polygon) jtsGeometry);
        } else if (jtsGeometry instanceof MultiPoint) {
            pbGeometry = serializeMultiPoint((MultiPoint) jtsGeometry);
        } else if (jtsGeometry instanceof MultiLineString) {
            pbGeometry = serializeMultiLineString((MultiLineString) jtsGeometry);
        }else if (jtsGeometry instanceof MultiPolygon) {
            pbGeometry = serializeMultiPolygon((MultiPolygon) jtsGeometry);
        }else if (jtsGeometry instanceof LineSegment) {
            pbGeometry = serializeLine((LineSegment) jtsGeometry);
        }else if (jtsGeometry instanceof Triangle) {
            pbGeometry = serializeTriangle((Triangle) jtsGeometry);
        }else if (jtsGeometry instanceof GeometryCollection) {
            pbGeometry = serializeGeometryCollection((GeometryCollection) jtsGeometry);
        } else {
            throw new IllegalArgumentException("Unsupported Geometric type for Protobuf Serialization");
        }
        pbGeometry.writeDelimitedTo(output);
    }

    public void close() throws IOException {
        output.close();
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

    private GeoProtobuf.Geometry serializeLinearRing(LineString jtsLinearRing) throws IOException {
        if (jtsLinearRing.getCoordinates().length < 2) {
            throw new IllegalArgumentException("Insufficient Coordinates");
        } else {
            GeoProtobuf.Geometry.Builder geoLinearRing = GeoProtobuf.Geometry.newBuilder();
            geoLinearRing.setType(GeoProtobuf.Geometry.Type.LINEARRING);
            for (Coordinate coord : jtsLinearRing.getCoordinates()) {
                geoLinearRing.addCoordinates(createCoordinate(coord));
            }
            return geoLinearRing.build();
        }
    }

    private GeoProtobuf.Geometry serializeLine(LineSegment jtsLineSegment) throws IOException {
        Coordinate p0 = jtsLineSegment.getCoordinate(0);
        Coordinate p1 = jtsLineSegment.getCoordinate(1);
        if (p0 == null || p1 == null) {
            throw new IllegalArgumentException("Insufficient Coordinates");
        } else {
            GeoProtobuf.Geometry.Builder geoLine = GeoProtobuf.Geometry.newBuilder();
            geoLine.setType(GeoProtobuf.Geometry.Type.LINE);
            geoLine.addCoordinates(createCoordinate(p0));
            geoLine.addCoordinates(createCoordinate(p1));
            return geoLine.build();
        }
    }

    public GeoProtobuf.Geometry serializeMultiPolygon(MultiPolygon jtsMultiPolygon) throws IOException {
        GeoProtobuf.Geometry.Builder geoMultiPolygon = GeoProtobuf.Geometry.newBuilder();
        geoMultiPolygon.setType(GeoProtobuf.Geometry.Type.MULTIPOLYGON);
        for (int i = 0; i < jtsMultiPolygon.getNumGeometries(); i++) {
            geoMultiPolygon.addGeometries(serializePolygon((Polygon) jtsMultiPolygon.getGeometryN(i)));
        }
        return geoMultiPolygon.build();
    }

    private GeoProtobuf.Geometry serializeTriangle(Triangle jtsTriangle) throws IOException {
        Coordinate p0 = jtsTriangle.p0;
        Coordinate p1 = jtsTriangle.p1;
        Coordinate p2 = jtsTriangle.p2;
        if (p0 == null || p1 == null || p2 == null) {
            throw new IllegalArgumentException("Insufficient Coordinates");
        } else {
            GeoProtobuf.Geometry.Builder geoTriangle = GeoProtobuf.Geometry.newBuilder();
            geoTriangle.setType(GeoProtobuf.Geometry.Type.TRIANGLE);
            geoTriangle.addCoordinates(createCoordinate(p0));
            geoTriangle.addCoordinates(createCoordinate(p1));
            geoTriangle.addCoordinates(createCoordinate(p2));
            return geoTriangle.build();
        }
    }

    private GeoProtobuf.Coordinate createCoordinate(Coordinate jtsCoordinate) {
        GeoProtobuf.Coordinate.Builder coordinate = GeoProtobuf.Coordinate.newBuilder();
        coordinate.setX(jtsCoordinate.x);
        coordinate.setY(jtsCoordinate.y);
        coordinate.setZ(jtsCoordinate.z);
        return coordinate.build();
    }

    private GeoProtobuf.Geometry serializeGeometryCollection(GeometryCollection jtsGeoCollection) throws IOException {
        GeoProtobuf.Geometry.Builder geoCollection = GeoProtobuf.Geometry.newBuilder();
        geoCollection.setType(GeoProtobuf.Geometry.Type.GEOMETRYCOLLECTION);
        for (int i = 0; i < jtsGeoCollection.getNumGeometries(); i++) {
            Geometry jtsGeometry = jtsGeoCollection.getGeometryN(i);
            if (jtsGeometry instanceof Point) {
                geoCollection.addGeometries(serializePoint((Point) jtsGeometry));
            }else if (jtsGeometry instanceof LinearRing) {
                geoCollection.addGeometries(serializeLinearRing((LinearRing) jtsGeometry));
            } else if (jtsGeometry instanceof LineString) {
                geoCollection.addGeometries(serializeLineString((LineString) jtsGeometry));
            } else if (jtsGeometry instanceof Polygon) {
                geoCollection.addGeometries(serializePolygon((Polygon) jtsGeometry));
            } else if (jtsGeometry instanceof MultiPoint) {
                geoCollection.addGeometries(serializeMultiPoint((MultiPoint) jtsGeometry));
            } else if (jtsGeometry instanceof MultiLineString) {
                geoCollection.addGeometries(serializeMultiLineString((MultiLineString) jtsGeometry));
            }else if (jtsGeometry instanceof MultiPolygon) {
                geoCollection.addGeometries(serializeMultiPolygon((MultiPolygon) jtsGeometry));
            } else {
                throw new IllegalArgumentException("Unsupported Geometric type for Protobuf Serialization");
            }
        }
        return geoCollection.build();
    }
}