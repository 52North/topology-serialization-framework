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

package org.n52.tsf.model.vector.jts;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.log4j.Logger;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Double.NaN;

/**
 * This class provides the JTS to avro Serialization functionality.
 */
public class AvroSerializationHandler {
    final static Logger logger = Logger.getLogger(AvroSerializationHandler.class);

    public void serialize(Geometry jtsGeometry, OutputStream outputStream) throws IOException {
        org.n52.tsf.serialization.avro.gen.vector.Geometry avroGeometry;
        if (jtsGeometry instanceof Point) {
            avroGeometry = serializePoint((Point) jtsGeometry);
        } else if (jtsGeometry instanceof LinearRing) {
            avroGeometry = serializeLinearRing((LinearRing) jtsGeometry);
        } else if (jtsGeometry instanceof LineString) {
            avroGeometry = serializeLineString((LineString) jtsGeometry);
        } else if (jtsGeometry instanceof Polygon) {
            avroGeometry = serializePolygon((Polygon) jtsGeometry);
        } else if (jtsGeometry instanceof MultiPoint) {
            avroGeometry = serializeMultiPoint((MultiPoint) jtsGeometry);
        } else if (jtsGeometry instanceof MultiLineString) {
            avroGeometry = serializeMultiLineString((MultiLineString) jtsGeometry);
        } else if (jtsGeometry instanceof MultiPolygon) {
            avroGeometry = serializeMultiPolygon((MultiPolygon) jtsGeometry);
        } else {
            throw new IllegalArgumentException("Unsupported Geometric type for Avro Serialization");
        }

        DatumWriter<org.n52.tsf.serialization.avro.gen.vector.Geometry> datumWriter = new SpecificDatumWriter<>(org.n52.tsf.serialization.avro.gen.vector.Geometry.class);
        DataFileWriter<org.n52.tsf.serialization.avro.gen.vector.Geometry> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(avroGeometry.getSchema(), outputStream);
        dataFileWriter.append(avroGeometry);
        dataFileWriter.close();
    }

    public void serialize(LineSegment jtsLine, OutputStream outputStream) throws IOException {
        org.n52.tsf.serialization.avro.gen.vector.Geometry avroGeometry = serializeLine(jtsLine);
        DatumWriter<org.n52.tsf.serialization.avro.gen.vector.Geometry> datumWriter = new SpecificDatumWriter<>(org.n52.tsf.serialization.avro.gen.vector.Geometry.class);
        DataFileWriter<org.n52.tsf.serialization.avro.gen.vector.Geometry> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(avroGeometry.getSchema(), outputStream);
        dataFileWriter.append(avroGeometry);
        dataFileWriter.close();
    }

    public void serialize(Triangle jtsTriangle, OutputStream outputStream) throws IOException {
        org.n52.tsf.serialization.avro.gen.vector.Geometry avroGeometry = serializeTriangle(jtsTriangle);
        DatumWriter<org.n52.tsf.serialization.avro.gen.vector.Geometry> datumWriter = new SpecificDatumWriter<>(org.n52.tsf.serialization.avro.gen.vector.Geometry.class);
        DataFileWriter<org.n52.tsf.serialization.avro.gen.vector.Geometry> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(avroGeometry.getSchema(), outputStream);
        dataFileWriter.append(avroGeometry);
        dataFileWriter.close();
    }

    private org.n52.tsf.serialization.avro.gen.vector.Geometry serializePoint(Point jtsPoint) {
        if (jtsPoint.getCoordinates().length == 0) {
            throw new IllegalArgumentException("No Coordinate data available");
        } else {
            List<org.n52.tsf.serialization.avro.gen.vector.Coordinate> coordinateList = new ArrayList<>();
            coordinateList.add(createCoordinate(jtsPoint.getCoordinate()));
            return createGeometry(coordinateList, null, org.n52.tsf.serialization.avro.gen.vector.Type.POINT);
        }
    }

    private org.n52.tsf.serialization.avro.gen.vector.Geometry serializeLineString(LineString jtsLineString) {
        if (jtsLineString.getCoordinates().length < 2) {
            throw new IllegalArgumentException("Insufficient Coordinates");
        } else {
            List<org.n52.tsf.serialization.avro.gen.vector.Coordinate> coordinateList = new ArrayList<>();
            for (Coordinate coord : jtsLineString.getCoordinates()) {
                coordinateList.add(createCoordinate(coord));
            }
            return createGeometry(coordinateList, null, org.n52.tsf.serialization.avro.gen.vector.Type.LINESTRING);
        }
    }

    private org.n52.tsf.serialization.avro.gen.vector.Geometry serializeMultiPoint(MultiPoint jtsMultiPoint) throws IOException {
        List<org.n52.tsf.serialization.avro.gen.vector.Geometry> geometries = new ArrayList<>();
        for (int i = 0; i < jtsMultiPoint.getNumGeometries(); i++) {
            geometries.add(serializePoint((Point) jtsMultiPoint.getGeometryN(i)));
        }
        return createGeometry(null, geometries, org.n52.tsf.serialization.avro.gen.vector.Type.MULTIPOINT);
    }

    private org.n52.tsf.serialization.avro.gen.vector.Geometry serializeMultiLineString(MultiLineString jtsMultiLineString) throws IOException {
        List<org.n52.tsf.serialization.avro.gen.vector.Geometry> geometries = new ArrayList<>();
        for (int i = 0; i < jtsMultiLineString.getNumGeometries(); i++) {
            geometries.add(serializeLineString((LineString) jtsMultiLineString.getGeometryN(i)));
        }
        return createGeometry(null, geometries, org.n52.tsf.serialization.avro.gen.vector.Type.MULTILINESTRING);
    }

    private org.n52.tsf.serialization.avro.gen.vector.Geometry serializeLinearRing(LineString jtsLinearRing) throws IOException {
        if (jtsLinearRing.getCoordinates().length < 2) {
            throw new IllegalArgumentException("Insufficient Coordinates");
        } else {
            List<org.n52.tsf.serialization.avro.gen.vector.Coordinate> coordinateList = new ArrayList<>();
            for (Coordinate coord : jtsLinearRing.getCoordinates()) {
                coordinateList.add(createCoordinate(coord));
            }
            return createGeometry(coordinateList, null, org.n52.tsf.serialization.avro.gen.vector.Type.LINEARRING);
        }
    }

    public org.n52.tsf.serialization.avro.gen.vector.Geometry serializeMultiPolygon(MultiPolygon jtsMultiPolygon) throws IOException {
        List<org.n52.tsf.serialization.avro.gen.vector.Geometry> geometries = new ArrayList<>();
        for (int i = 0; i < jtsMultiPolygon.getNumGeometries(); i++) {
            geometries.add(serializePolygon((Polygon) jtsMultiPolygon.getGeometryN(i)));
        }
        return createGeometry(null, geometries, org.n52.tsf.serialization.avro.gen.vector.Type.MULTIPOLYGON);
    }

    private org.n52.tsf.serialization.avro.gen.vector.Geometry serializePolygon(Polygon jtsPolygon) {
        LineString externalLS = jtsPolygon.getExteriorRing();
        int noOfInteriorRings = jtsPolygon.getNumInteriorRing();

        List<org.n52.tsf.serialization.avro.gen.vector.Coordinate> exCoordinateList = new ArrayList<>();
        for (Coordinate coord : externalLS.getCoordinates()) {
            exCoordinateList.add(createCoordinate(coord));
        }
        List<org.n52.tsf.serialization.avro.gen.vector.Geometry> interiorGeos = new ArrayList<>();
        if (noOfInteriorRings > 0) {
            for (int i = 0; i < noOfInteriorRings; i++) {
                LineString interiorLS = jtsPolygon.getInteriorRingN(i);
                List<org.n52.tsf.serialization.avro.gen.vector.Coordinate> inCoordinateList = new ArrayList<>();
                for (Coordinate coord : interiorLS.getCoordinates()) {
                    inCoordinateList.add(createCoordinate(coord));
                }
                interiorGeos.add(createGeometry(inCoordinateList, null, org.n52.tsf.serialization.avro.gen.vector.Type.LINEARRING));
            }
        }
        return createGeometry(exCoordinateList, interiorGeos, org.n52.tsf.serialization.avro.gen.vector.Type.POLYGON);
    }

    private org.n52.tsf.serialization.avro.gen.vector.Geometry serializeLine(LineSegment jtsLineSegment) throws IOException {
        Coordinate p0 = jtsLineSegment.getCoordinate(0);
        Coordinate p1 = jtsLineSegment.getCoordinate(1);
        if (p0 == null || p1 == null) {
            throw new IllegalArgumentException("Insufficient Coordinates");
        } else {
            List<org.n52.tsf.serialization.avro.gen.vector.Coordinate> coordinateList = new ArrayList<>();
            coordinateList.add(createCoordinate(p0));
            coordinateList.add(createCoordinate(p1));
            return createGeometry(coordinateList, null, org.n52.tsf.serialization.avro.gen.vector.Type.LINE);
        }
    }

    private org.n52.tsf.serialization.avro.gen.vector.Geometry serializeTriangle(Triangle jtsTriangle) throws IOException {
        Coordinate p0 = jtsTriangle.p0;
        Coordinate p1 = jtsTriangle.p1;
        Coordinate p2 = jtsTriangle.p2;
        if (p0 == null || p1 == null || p2 == null) {
            throw new IllegalArgumentException("Insufficient Coordinates");
        } else {
            List<org.n52.tsf.serialization.avro.gen.vector.Coordinate> coordinateList = new ArrayList<>();
            coordinateList.add(createCoordinate(p0));
            coordinateList.add(createCoordinate(p1));
            coordinateList.add(createCoordinate(p2));
            return createGeometry(coordinateList, null, org.n52.tsf.serialization.avro.gen.vector.Type.TRIANGLE);
        }
    }

    private org.n52.tsf.serialization.avro.gen.vector.Coordinate createCoordinate(Coordinate jtsCoordinate) {
        org.n52.tsf.serialization.avro.gen.vector.Coordinate.Builder coordinate = org.n52.tsf.serialization.avro.gen.vector.Coordinate.newBuilder();
        coordinate.setX(jtsCoordinate.x);
        coordinate.setY(jtsCoordinate.y);
        coordinate.setZ(NaN);
        return coordinate.build();
    }

    private org.n52.tsf.serialization.avro.gen.vector.Geometry createGeometry(List<org.n52.tsf.serialization.avro.gen.vector.Coordinate> coordinates,
                                                                       List<org.n52.tsf.serialization.avro.gen.vector.Geometry> geometries,
                                                                       org.n52.tsf.serialization.avro.gen.vector.Type type) {
        org.n52.tsf.serialization.avro.gen.vector.Geometry.Builder geometry = org.n52.tsf.serialization.avro.gen.
                vector.Geometry.newBuilder();
        if (coordinates == null) {
            List<org.n52.tsf.serialization.avro.gen.vector.Coordinate> coordinateList = new ArrayList<>();
            geometry.setCoordinates(coordinateList);
        } else {
            geometry.setCoordinates(coordinates);
        }
        if (geometries == null) {
            List<org.n52.tsf.serialization.avro.gen.vector.Geometry> geometryList = new ArrayList<>();
            geometry.setGeometries(geometryList);
        } else {
            geometry.setGeometries(geometries);
        }

        geometry.setType(type);
        return geometry.build();
    }
}
