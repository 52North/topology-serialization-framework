package org.n52.tsf.model.vector.jts;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.log4j.Logger;
import org.locationtech.jts.geom.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class provides the  Avro to JTS deserialization functionality.
 */
public class AvroDeserializationHandler {
    final static Logger logger = Logger.getLogger(AvroDeserializationHandler.class);
    GeometryFactory geometryFactory;

    public AvroDeserializationHandler() {
        this.geometryFactory = new GeometryFactory();
    }

    public Geometry deserialize(InputStream inputStream) throws IOException {
        DatumReader<org.n52.tsf.serialization.avro.gen.vector.Geometry> datumReader =
                new SpecificDatumReader<>(org.n52.tsf.serialization.avro.gen.vector.Geometry.class);
        DataFileStream<org.n52.tsf.serialization.avro.gen.vector.Geometry> dataFileReader = new DataFileStream<>(inputStream, datumReader);
        org.n52.tsf.serialization.avro.gen.vector.Geometry avroGeometry = null;
        if (dataFileReader.hasNext()) {
            avroGeometry = dataFileReader.next();
        }

        Geometry jtsGeometry = null;
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
            default:
                logger.error("Unsupported Geometric type for Avro deserialization");
        }
        return jtsGeometry;
    }

    private Point deserializePoint(org.n52.tsf.serialization.avro.gen.vector.Geometry avroGeometry) {
        List<org.n52.tsf.serialization.avro.gen.vector.Coordinate> coordinates = avroGeometry.getCoordinates();
        Coordinate jtsCoordinate = createJtsCoordinate(coordinates.get(0));
        Point point = geometryFactory.createPoint(jtsCoordinate);
        return point;
    }

    public LineSegment deserializeLine(InputStream inputStream) throws IOException {
        DatumReader<org.n52.tsf.serialization.avro.gen.vector.Geometry> datumReader =
                new SpecificDatumReader<>(org.n52.tsf.serialization.avro.gen.vector.Geometry.class);
        DataFileStream<org.n52.tsf.serialization.avro.gen.vector.Geometry> dataFileReader = new DataFileStream<>(inputStream, datumReader);
        org.n52.tsf.serialization.avro.gen.vector.Geometry avroGeometry = null;
        if (dataFileReader.hasNext()) {
            avroGeometry = dataFileReader.next();
        }
        Coordinate[] jtsCoordinates = avroGeometry.getCoordinates().
                stream().map(this::createJtsCoordinate).collect(Collectors.toList()).stream().toArray(Coordinate[]::new);
        LineSegment lineSegment = new LineSegment(jtsCoordinates[0], jtsCoordinates[1]);
        return lineSegment;
    }

    public Triangle deserializeTriangle(InputStream inputStream) throws IOException {
        DatumReader<org.n52.tsf.serialization.avro.gen.vector.Geometry> datumReader =
                new SpecificDatumReader<>(org.n52.tsf.serialization.avro.gen.vector.Geometry.class);
        DataFileStream<org.n52.tsf.serialization.avro.gen.vector.Geometry> dataFileReader = new DataFileStream<>(inputStream, datumReader);
        org.n52.tsf.serialization.avro.gen.vector.Geometry avroGeometry = null;
        if (dataFileReader.hasNext()) {
            avroGeometry = dataFileReader.next();
        }
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
