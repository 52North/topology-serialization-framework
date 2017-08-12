package org.n52.tsf.model.vector.jts.test;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.n52.tsf.model.vector.jts.AvroDeserializationHandler;
import org.n52.tsf.model.vector.jts.AvroSerializationHandler;
import org.n52.tsf.model.vector.jts.PBDeserializationHandler;
import org.n52.tsf.model.vector.jts.PBSerializationHandler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JTSModelGeoCollectionTest {

    @Before
    public void setUp() throws Exception {
        Path filePath = Paths.get(Utils.TEST_FILE_LOCATION);
        Files.createFile(filePath);
    }

    @Test
    public void testSerializeGeoCollection() throws Exception {
        GeometryFactory geometryFactory = new GeometryFactory();
        Polygon polygon = geometryFactory.createPolygon(new Coordinate[]{
                new Coordinate(0, 0), new Coordinate(10, 0), new Coordinate(0, 10), new Coordinate(10, 10), new Coordinate(0, 0)});
        Point point = geometryFactory.createPoint(new Coordinate(1, 2));
        GeometryCollection geometryCollection = geometryFactory.createGeometryCollection(new Geometry[]{point, polygon});
        System.out.println("-------------- Serializing JTS Model Geometry Collection via Protobuf -------------------------");
        PBSerializationHandler pbSerializer = new PBSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);
        try {
            pbSerializer.serialize(geometryCollection, output);
        } finally {
            output.close();
        }
        assertTrue(new File(Utils.TEST_FILE_LOCATION).length() > 0);
        System.out.println("Successfully Serialized....");
    }

    @Test
    public void testDeserializeGeoCollection() throws Exception {
        GeometryFactory geometryFactory = new GeometryFactory();
        Polygon polygon = geometryFactory.createPolygon(new Coordinate[]{
                new Coordinate(0, 0), new Coordinate(10, 0), new Coordinate(0, 10), new Coordinate(10, 10), new Coordinate(0, 0)});
        Point point = geometryFactory.createPoint(new Coordinate(1, 2));
        GeometryCollection geometryCollection = geometryFactory.createGeometryCollection(new Geometry[]{point, polygon});
        PBSerializationHandler pbSerializer = new PBSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);
        try {
            pbSerializer.serialize(geometryCollection, output);
            System.out.println("-------------- Deserializing JTS Model Geometry Collection via Protobuf ------------------------");
            PBDeserializationHandler pbDeserializationHandler = new PBDeserializationHandler();

            GeometryCollection geoCollDeserialized = (GeometryCollection) pbDeserializationHandler.deserialize(new FileInputStream(Utils.TEST_FILE_LOCATION));
            assertEquals(geometryCollection, geoCollDeserialized);
            System.out.println("Successfully Deserialized : " + geoCollDeserialized);
        } finally {
            output.close();
        }
    }

    @Test
    public void testSerializeGeoCollectionWithAvro() throws Exception {
        GeometryFactory geometryFactory = new GeometryFactory();
        Polygon polygon = geometryFactory.createPolygon(new Coordinate[]{
                new Coordinate(0, 0), new Coordinate(10, 0), new Coordinate(0, 10), new Coordinate(10, 10), new Coordinate(0, 0)});
        Point point = geometryFactory.createPoint(new Coordinate(1, 2));
        GeometryCollection geometryCollection = geometryFactory.createGeometryCollection(new Geometry[]{point, polygon});
        System.out.println("-------------- Serializing JTS Model Geometry Collection via Avro -------------------------");
        PBSerializationHandler pbSerializer = new PBSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);
        try {
            pbSerializer.serialize(geometryCollection, output);
        } finally {
            output.close();
        }
        assertTrue(new File(Utils.TEST_FILE_LOCATION).length() > 0);
        System.out.println("Successfully Serialized....");
    }

    @Test
    public void testDeserializeGeoCollectionWithAvro() throws Exception {
        GeometryFactory geometryFactory = new GeometryFactory();
        Polygon polygon = geometryFactory.createPolygon(new Coordinate[]{
                new Coordinate(0, 0), new Coordinate(10, 0), new Coordinate(0, 10), new Coordinate(10, 10), new Coordinate(0, 0)});
        Point point = geometryFactory.createPoint(new Coordinate(1, 2));
        GeometryCollection geometryCollection = geometryFactory.createGeometryCollection(new Geometry[]{point, polygon});
        AvroSerializationHandler avroSerializer = new AvroSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);
        try {
            avroSerializer.serialize(geometryCollection, output);
            System.out.println("-------------- Deserializing JTS Model Geometry Collection via Avro ------------------------");
            AvroDeserializationHandler avroDeserializationHandler = new AvroDeserializationHandler();

            GeometryCollection geoCollDeserialized = (GeometryCollection) avroDeserializationHandler.deserialize(new FileInputStream(Utils.TEST_FILE_LOCATION));
            assertEquals(geometryCollection, geoCollDeserialized);
            System.out.println("Successfully Deserialized : " + geoCollDeserialized);
        } finally {
            output.close();
        }
    }

    @After
    public void tearDown() throws Exception {
        Path filePath = Paths.get(Utils.TEST_FILE_LOCATION);
        Files.deleteIfExists(filePath);
    }
}
