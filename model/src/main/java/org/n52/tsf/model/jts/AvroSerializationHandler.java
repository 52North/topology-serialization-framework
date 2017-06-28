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

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.log4j.Logger;
import org.locationtech.jts.geom.*;
import org.n52.tsf.serialization.avro.gen.Type;

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
        org.n52.tsf.serialization.avro.gen.Geometry avroGeometry;
        if (jtsGeometry instanceof Point) {
            avroGeometry = serializePoint((Point) jtsGeometry);
        } else if (jtsGeometry instanceof LineString) {
            avroGeometry = serializeLineString((LineString) jtsGeometry);
        } else if (jtsGeometry instanceof Polygon) {
            avroGeometry = serializePolygon((Polygon) jtsGeometry);
        } else {
            throw new IllegalArgumentException("Unsupported Geometric type");
        }

        DatumWriter<org.n52.tsf.serialization.avro.gen.Geometry> datumWriter = new SpecificDatumWriter<>(org.n52.tsf.serialization.avro.gen.Geometry.class);
        DataFileWriter<org.n52.tsf.serialization.avro.gen.Geometry> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(avroGeometry.getSchema(), outputStream);
        dataFileWriter.append(avroGeometry);
        dataFileWriter.close();
    }

    private org.n52.tsf.serialization.avro.gen.Geometry serializePoint(Point jtsPoint) {
        if (jtsPoint.getCoordinates().length == 0) {
            throw new IllegalArgumentException("No Coordinate data available");
        } else {
            org.n52.tsf.serialization.avro.gen.Geometry.Builder geoPoint = org.n52.tsf.serialization.avro.gen.
                    Geometry.newBuilder();
            geoPoint.setType(org.n52.tsf.serialization.avro.gen.Type.POINT);
            List<org.n52.tsf.serialization.avro.gen.Coordinate> coordinateList = new ArrayList<>();
            coordinateList.add(createCoordinate(jtsPoint.getCoordinate()));
            geoPoint.setCoordinates(coordinateList);
            List<org.n52.tsf.serialization.avro.gen.Geometry> geometryList = new ArrayList<>();
            geoPoint.setGeometries(geometryList);
            return geoPoint.build();
        }
    }

    private org.n52.tsf.serialization.avro.gen.Geometry serializeLineString(LineString jtsLineString) {
        if (jtsLineString.getCoordinates().length < 2) {
            throw new IllegalArgumentException("Insufficient Coordinates");
        } else {
            List<org.n52.tsf.serialization.avro.gen.Coordinate> coordinateList = new ArrayList<>();
            for (Coordinate coord : jtsLineString.getCoordinates()) {
                coordinateList.add(createCoordinate(coord));
            }
            return createGeometry(coordinateList, null, org.n52.tsf.serialization.avro.gen.Type.LINESTRING);
        }
    }

    private org.n52.tsf.serialization.avro.gen.Geometry serializePolygon(Polygon jtsPolygon) {
        LineString externalLS = jtsPolygon.getExteriorRing();
        int noOfInteriorRings = jtsPolygon.getNumInteriorRing();

        List<org.n52.tsf.serialization.avro.gen.Coordinate> exCoordinateList = new ArrayList<>();
        for (Coordinate coord : externalLS.getCoordinates()) {
            exCoordinateList.add(createCoordinate(coord));
        }
        List<org.n52.tsf.serialization.avro.gen.Geometry> interiorGeos = new ArrayList<>();
        if (noOfInteriorRings > 0) {
            for (int i = 0; i < noOfInteriorRings; i++) {
                LineString interiorLS = jtsPolygon.getInteriorRingN(i);
                List<org.n52.tsf.serialization.avro.gen.Coordinate> inCoordinateList = new ArrayList<>();
                for (Coordinate coord : interiorLS.getCoordinates()) {
                    inCoordinateList.add(createCoordinate(coord));
                }
                interiorGeos.add(createGeometry(inCoordinateList, null, Type.LINEARRING));
            }
        }

        return createGeometry(exCoordinateList, interiorGeos, Type.POLYGON);
    }

    private org.n52.tsf.serialization.avro.gen.Coordinate createCoordinate(Coordinate jtsCoordinate) {
        org.n52.tsf.serialization.avro.gen.Coordinate.Builder coordinate = org.n52.tsf.serialization.avro.gen.Coordinate.newBuilder();
        coordinate.setX(jtsCoordinate.x);
        coordinate.setY(jtsCoordinate.y);
        coordinate.setZ(NaN);
        return coordinate.build();
    }

    private org.n52.tsf.serialization.avro.gen.Geometry createGeometry(List<org.n52.tsf.serialization.avro.gen.Coordinate> coordinates,
                                                                       List<org.n52.tsf.serialization.avro.gen.Geometry> geometries,
                                                                       org.n52.tsf.serialization.avro.gen.Type type) {
        org.n52.tsf.serialization.avro.gen.Geometry.Builder geometry = org.n52.tsf.serialization.avro.gen.
                Geometry.newBuilder();
        if (coordinates == null) {
            List<org.n52.tsf.serialization.avro.gen.Coordinate> coordinateList = new ArrayList<>();
            geometry.setCoordinates(coordinateList);
        } else {
            geometry.setCoordinates(coordinates);
        }
        if (geometries == null) {
            List<org.n52.tsf.serialization.avro.gen.Geometry> geometryList = new ArrayList<>();
            geometry.setGeometries(geometryList);
        } else {
            geometry.setGeometries(geometries);
        }

        geometry.setType(type);
        return geometry.build();
    }
}
