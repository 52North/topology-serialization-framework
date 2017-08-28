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

public class JTSModelMultiLineStringTest {
    @Before
    public void setUp() throws Exception {
        Path filePath = Paths.get(Utils.TEST_FILE_LOCATION);
        Files.createFile(filePath);
    }

    @Test
    public void testGeoMultiLineStringWithProtobof() throws Exception {
        GeometryFactory geometryFactory = new GeometryFactory();
        LineString lineString1 = geometryFactory.createLineString(new Coordinate[]{
                new Coordinate(0, 0), new Coordinate(1, 0), new Coordinate(1, 1)});
        LineString lineString2 = geometryFactory.createLineString(new Coordinate[]{
                new Coordinate(1, 1), new Coordinate(2, 1), new Coordinate(2, 2)});
        MultiLineString multiLineString = geometryFactory.createMultiLineString(new LineString[]{lineString1, lineString2});
        System.out.println("-------------- Serializing JTS Model MultiLineString via Protobuf -------------------------");
        SerializationHandler pbSerializer = SerializationFactory.createSerializer(new FileOutputStream(Utils.TEST_FILE_LOCATION), SerializerType.PROTOBUF_SERIALIZER_LT);
        pbSerializer.serialize(multiLineString);
        pbSerializer.close();
        assertTrue(new File(Utils.TEST_FILE_LOCATION).length() > 0);
        System.out.println("-------------- Deserializing JTS Model MultiLineString via Protobuf -------------------------");
        DeserializationHandler pbDeserializationHandler = DeserializationFactory.createDeserializer(new FileInputStream(Utils.TEST_FILE_LOCATION), DeserializerType.PROTOBUF_DESERIALIZER_LT);
        MultiLineString multiLineStringDeserialized = (MultiLineString) pbDeserializationHandler.deserialize();
        pbDeserializationHandler.close();
        assertEquals(multiLineString, multiLineStringDeserialized);
        System.out.println("Successfully Deserialized : " + multiLineStringDeserialized);

    }

    @Test
    public void testGeoMultiLineStringWithAvro() throws Exception {
        GeometryFactory geometryFactory = new GeometryFactory();
        LineString lineString1 = geometryFactory.createLineString(new Coordinate[]{
                new Coordinate(0, 0), new Coordinate(1, 0), new Coordinate(1, 1)});
        LineString lineString2 = geometryFactory.createLineString(new Coordinate[]{
                new Coordinate(1, 1), new Coordinate(2, 1), new Coordinate(2, 2)});
        MultiLineString multiLineString = geometryFactory.createMultiLineString(new LineString[]{lineString1, lineString2});
        System.out.println("-------------- Serializing JTS Model MultiLineString via Avro -------------------------");
        SerializationHandler avroSerializer = SerializationFactory.createSerializer(new FileOutputStream(Utils.TEST_FILE_LOCATION), SerializerType.AVRO_SERIALIZER_LT);
        avroSerializer.serialize(multiLineString);
        avroSerializer.close();
        assertTrue(new File(Utils.TEST_FILE_LOCATION).length() > 0);
        System.out.println("-------------- Deserializing JTS Model MultiLineString via Avro -------------------------");
        DeserializationHandler avroDeserializationHandler = DeserializationFactory.createDeserializer(new FileInputStream(Utils.TEST_FILE_LOCATION), DeserializerType.AVRO_DESERIALIZER_LT);
        MultiLineString multiLineStringDeserialized = (MultiLineString) avroDeserializationHandler.deserialize();
        avroDeserializationHandler.close();
        assertEquals(multiLineString, multiLineStringDeserialized);
        System.out.println("Successfully Deserialized : " + multiLineStringDeserialized);
    }

    @After
    public void tearDown() throws Exception {
        Path filePath = Paths.get(Utils.TEST_FILE_LOCATION);
        Files.deleteIfExists(filePath);
    }
}
