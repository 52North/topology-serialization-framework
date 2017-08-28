package org.n52.tsf.model.vector.jts.test;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.n52.tsf.model.*;

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
    public void testGeoCollectionWithProtoBuf() throws Exception {
        GeometryFactory geometryFactory = new GeometryFactory();
        Polygon polygon = geometryFactory.createPolygon(new Coordinate[]{
                new Coordinate(0, 0), new Coordinate(10, 0), new Coordinate(0, 10), new Coordinate(10, 10), new Coordinate(0, 0)});
        Point point = geometryFactory.createPoint(new Coordinate(1, 2));
        GeometryCollection geometryCollection = geometryFactory.createGeometryCollection(new Geometry[]{point, polygon});
        System.out.println("-------------- Serializing JTS Model Geometry Collection via Protobuf -------------------------");
        SerializationHandler pbSerializer = SerializationFactory.createSerializer(new FileOutputStream(Utils.TEST_FILE_LOCATION), SerializerType.PROTOBUF_SERIALIZER_LT);
        pbSerializer.serialize(geometryCollection);
        assertTrue(new File(Utils.TEST_FILE_LOCATION).length() > 0);
        System.out.println("-------------- Deserializing JTS Model Geometry Collection via Protobuf ------------------------");
        DeserializationHandler pbDeserializationHandler = DeserializationFactory.createDeserializer(new FileInputStream(Utils.TEST_FILE_LOCATION), DeserializerType.PROTOBUF_DESERIALIZER_LT);
        GeometryCollection geoCollDeserialized = (GeometryCollection) pbDeserializationHandler.deserialize();
        assertEquals(geometryCollection, geoCollDeserialized);
        System.out.println("Successfully Deserialized : " + geoCollDeserialized);
    }

    @Test
    public void testGeoCollectionWithAvro() throws Exception {
        GeometryFactory geometryFactory = new GeometryFactory();
        Polygon polygon = geometryFactory.createPolygon(new Coordinate[]{
                new Coordinate(0, 0), new Coordinate(10, 0), new Coordinate(0, 10), new Coordinate(10, 10), new Coordinate(0, 0)});
        Point point = geometryFactory.createPoint(new Coordinate(1, 2));
        GeometryCollection geometryCollection = geometryFactory.createGeometryCollection(new Geometry[]{point, polygon});
        System.out.println("-------------- Serializing JTS Model Geometry Collection via Avro -------------------------");
        SerializationHandler avroSerializer = SerializationFactory.createSerializer(new FileOutputStream(Utils.TEST_FILE_LOCATION), SerializerType.AVRO_SERIALIZER_LT);
        avroSerializer.serialize(geometryCollection);
        avroSerializer.close();
        assertTrue(new File(Utils.TEST_FILE_LOCATION).length() > 0);
        System.out.println("-------------- Deserializing JTS Model Geometry Collection via Avro ------------------------");
        DeserializationHandler avroDeserializationHandler = DeserializationFactory.createDeserializer(new FileInputStream(Utils.TEST_FILE_LOCATION), DeserializerType.AVRO_DESERIALIZER_LT);
        GeometryCollection geoCollDeserialized = (GeometryCollection) avroDeserializationHandler.deserialize();
        avroDeserializationHandler.close();
        assertEquals(geometryCollection, geoCollDeserialized);
        System.out.println("Successfully Deserialized : " + geoCollDeserialized);
    }

    @After
    public void tearDown() throws Exception {
        Path filePath = Paths.get(Utils.TEST_FILE_LOCATION);
        Files.deleteIfExists(filePath);
    }
}
