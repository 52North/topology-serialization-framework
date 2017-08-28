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

package org.n52.tsf.model.vector.jts.vividsolutions;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.log4j.Logger;
import com.vividsolutions.jts.geom.*;
import org.n52.tsf.model.DeserializationHandler;
import org.n52.tsf.model.DeserializerType;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class provides the  Avro to JTS deserialization functionality.
 */
public class AvroDeserializationHandlerVS extends DeserializationHandler{
    private final static Logger logger = Logger.getLogger(AvroDeserializationHandlerVS.class);
    private GeometryFactory geometryFactory;
    private DataFileStream<org.n52.tsf.serialization.avro.gen.vector.Geometry> dataFileReader;

    public AvroDeserializationHandlerVS(InputStream inputStream) throws IOException {
        super(DeserializerType.AVRO_DESERIALIZER_VS);
        DatumReader<org.n52.tsf.serialization.avro.gen.vector.Geometry> datumReader =
                new SpecificDatumReader<>(org.n52.tsf.serialization.avro.gen.vector.Geometry.class);
        dataFileReader = new DataFileStream<>(inputStream, datumReader);
        this.geometryFactory = new GeometryFactory();
    }

    public Object deserialize() {
        org.n52.tsf.serialization.avro.gen.vector.Geometry avroGeometry;
        Object jtsGeometry = null;
        if (dataFileReader.hasNext()) {
            avroGeometry = dataFileReader.next();
            switch (avroGeometry.getType()) {
                case POINT:
                    jtsGeometry = deserializePoint(avroGeometry);
                    break;
                case LINESTRING:
                    jtsGeometry = deserializeLineString(avroGeometry);
                    break;
                case POLYGON:
                    jtsGeometry = deserializePolygon(avroGeometry);
                    break;
                case MULTIPOINT:
                    jtsGeometry = deserializeMultiPoint(avroGeometry);
                    break;
                case MULTILINESTRING:
                    jtsGeometry = deserializeMultiLineString(avroGeometry);
                    break;
                case LINEARRING:
                    jtsGeometry = deserializeLinearRing(avroGeometry);
                    break;
                case MULTIPOLYGON:
                    jtsGeometry = deserializeMultiPolygon(avroGeometry);
                    break;
                case GEOMETRYCOLLECTION:
                    jtsGeometry = deserializeGeoCollection(avroGeometry);
                    break;
                case TRIANGLE:
                    jtsGeometry = deserializeTriangle(avroGeometry);
                    break;
                case LINE:
                    jtsGeometry = deserializeLine(avroGeometry);
                    break;
                default:
                    logger.error("Unsupported Geometric type for Avro deserialization");
            }
        }
        return jtsGeometry;
    }

    public void close() throws IOException {
        dataFileReader.close();
    }

    private Point deserializePoint(org.n52.tsf.serialization.avro.gen.vector.Geometry avroGeometry) {
        List<org.n52.tsf.serialization.avro.gen.vector.Coordinate> coordinates = avroGeometry.getCoordinates();
        Coordinate jtsCoordinate = createJtsCoordinate(coordinates.get(0));
        Point point = geometryFactory.createPoint(jtsCoordinate);
        return point;
    }

    private LineSegment deserializeLine(org.n52.tsf.serialization.avro.gen.vector.Geometry avroGeometry) {
        if (dataFileReader.hasNext()) {
            avroGeometry = dataFileReader.next();
        }
        Coordinate[] jtsCoordinates = avroGeometry.getCoordinates().
                stream().map(this::createJtsCoordinate).collect(Collectors.toList()).stream().toArray(Coordinate[]::new);
        LineSegment lineSegment = new LineSegment(jtsCoordinates[0], jtsCoordinates[1]);
        return lineSegment;
    }

    private Triangle deserializeTriangle(org.n52.tsf.serialization.avro.gen.vector.Geometry avroGeometry) {
        Coordinate[] jtsCoordinates = avroGeometry.getCoordinates().
                stream().map(this::createJtsCoordinate).collect(Collectors.toList()).stream().toArray(Coordinate[]::new);
        Triangle triangle = new Triangle(jtsCoordinates[0], jtsCoordinates[1], jtsCoordinates[2]);
        return triangle;
    }

    private Coordinate createJtsCoordinate(org.n52.tsf.serialization.avro.gen.vector.Coordinate avroCoordinate) {
        Coordinate jtsCoordinate = new Coordinate(avroCoordinate.getX(), avroCoordinate.getY());
        return jtsCoordinate;
    }

    private LineString deserializeLineString(org.n52.tsf.serialization.avro.gen.vector.Geometry avroGeometry) {
        Coordinate[] jtsCoordinates = avroGeometry.getCoordinates().
                stream().map(this::createJtsCoordinate).collect(Collectors.toList()).stream().toArray(Coordinate[]::new);
        LineString lineString = geometryFactory.createLineString(jtsCoordinates);
        return lineString;
    }

    private LinearRing deserializeLinearRing(org.n52.tsf.serialization.avro.gen.vector.Geometry avroGeometry) {
        Coordinate[] jtsCoordinates = avroGeometry.getCoordinates().
                stream().map(this::createJtsCoordinate).collect(Collectors.toList()).stream().toArray(Coordinate[]::new);
        LinearRing linearRing = geometryFactory.createLinearRing(jtsCoordinates);
        return linearRing;
    }

    private Polygon deserializePolygon(org.n52.tsf.serialization.avro.gen.vector.Geometry avroGeometry) {
        List<org.n52.tsf.serialization.avro.gen.vector.Geometry> geometries = avroGeometry.getGeometries();
        Polygon polygon;
        if (geometries.size() == 0) {
            Coordinate[] jtsCoordinates = avroGeometry.getCoordinates().
                    stream().map(this::createJtsCoordinate).collect(Collectors.toList()).stream().toArray(Coordinate[]::new);
            polygon = geometryFactory.createPolygon(jtsCoordinates);

        } else {
            Coordinate[] exCoordinates = avroGeometry.getCoordinates().
                    stream().map(this::createJtsCoordinate).collect(Collectors.toList()).stream().toArray(Coordinate[]::new);

            LinearRing exteriorLR = geometryFactory.createLinearRing(exCoordinates);

            List<LinearRing> jtsInteriorLRs = new ArrayList();
            for (org.n52.tsf.serialization.avro.gen.vector.Geometry geo : geometries) {
                Coordinate[] inCoordinates = geo.getCoordinates().
                        stream().map(this::createJtsCoordinate).collect(Collectors.toList()).stream().toArray(Coordinate[]::new);
                jtsInteriorLRs.add(geometryFactory.createLinearRing(inCoordinates));
            }
            polygon = geometryFactory.createPolygon(exteriorLR, jtsInteriorLRs.stream().toArray(LinearRing[]::new));
        }
        return polygon;
    }

    private MultiPolygon deserializeMultiPolygon(org.n52.tsf.serialization.avro.gen.vector.Geometry avroGeometry) {
        Polygon[] jtsPolygons = avroGeometry.getGeometries().
                stream().map(this::deserializePolygon).collect(Collectors.toList()).stream().toArray(Polygon[]::new);
        MultiPolygon multiPolygon = geometryFactory.createMultiPolygon(jtsPolygons);
        return multiPolygon;
    }

    private MultiPoint deserializeMultiPoint(org.n52.tsf.serialization.avro.gen.vector.Geometry avroGeometry) {
        Point[] jtsPoints = avroGeometry.getGeometries().
                stream().map(this::deserializePoint).collect(Collectors.toList()).stream().toArray(Point[]::new);
        MultiPoint multiPoint = geometryFactory.createMultiPoint(jtsPoints);
        return multiPoint;
    }

    private MultiLineString deserializeMultiLineString(org.n52.tsf.serialization.avro.gen.vector.Geometry avroGeometry) {
        LineString[] jtsLineStrings = avroGeometry.getGeometries().
                stream().map(this::deserializeLineString).collect(Collectors.toList()).stream().toArray(LineString[]::new);
        MultiLineString multiLineString = geometryFactory.createMultiLineString(jtsLineStrings);
        return multiLineString;
    }


    private GeometryCollection deserializeGeoCollection(org.n52.tsf.serialization.avro.gen.vector.Geometry avroGeometry) {
        Geometry[] jtsGeometries = avroGeometry.getGeometries().
                stream().map(this::deserializeGeometry).collect(Collectors.toList()).stream().toArray(Geometry[]::new);
        return geometryFactory.createGeometryCollection(jtsGeometries);
    }

    private Geometry deserializeGeometry(org.n52.tsf.serialization.avro.gen.vector.Geometry avroGeometry) {
        switch (avroGeometry.getType()) {
            case POINT:
                return deserializePoint(avroGeometry);
            case LINESTRING:
                return deserializeLineString(avroGeometry);
            case POLYGON:
                return deserializePolygon(avroGeometry);
            case MULTIPOINT:
                return deserializeMultiPoint(avroGeometry);
            case MULTILINESTRING:
                return deserializeMultiLineString(avroGeometry);
            case LINEARRING:
                return deserializeLinearRing(avroGeometry);
            case MULTIPOLYGON:
                return deserializeMultiPolygon(avroGeometry);
            default:
                logger.error("Unsupported Geometric type for Avro deserialization");
                return null;
        }
    }
}
