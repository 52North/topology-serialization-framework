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

package org.n52.tsf.model.jts.test;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineSegment;
import org.locationtech.jts.geom.LinearRing;
import org.n52.tsf.model.jts.AvroDeserializationHandler;
import org.n52.tsf.model.jts.AvroSerializationHandler;
import org.n52.tsf.model.jts.PBDeserializationHandler;
import org.n52.tsf.model.jts.PBSerializationHandler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JTSModellineSegmentTest {

    @Before
    public void setUp() throws Exception {
        Path filePath = Paths.get(Utils.TEST_FILE_LOCATION);
        Files.createFile(filePath);
    }

    @Test
    public void testSerializeGeoLineSegment() throws Exception {
        LineSegment lineSegment = new LineSegment(
                new Coordinate(0, 0), new Coordinate(1, 0));
        System.out.println("-------------- Serializing JTS Model LineSegment via Protobuf -------------------------");
        PBSerializationHandler pbSerializer = new PBSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);
        try {
            pbSerializer.serialize(lineSegment, output);
        } finally {
            output.close();
        }
        assertTrue(new File(Utils.TEST_FILE_LOCATION).length() > 0);
        System.out.println("Successfully Serialized....");
    }

    @Test
    public void testDeserializeGeoLineSegment() throws Exception {
        LineSegment lineSegment = new LineSegment(
                new Coordinate(0, 0), new Coordinate(1, 0));
        PBSerializationHandler pbSerializer = new PBSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);
        try {
            pbSerializer.serialize(lineSegment, output);
            System.out.println("-------------- Deserializing JTS Model LineSegment via Protobuf -------------------------");
            PBDeserializationHandler pbDeserializationHandler = new PBDeserializationHandler();
            LineSegment lineSegmentDeserialized = pbDeserializationHandler.deserializeLine(new FileInputStream(Utils.TEST_FILE_LOCATION));
            assertEquals(lineSegment, lineSegmentDeserialized);
            System.out.println("Successfully Deserialized : " + lineSegmentDeserialized);
        } finally {
            output.close();
        }
    }

    @Test
    public void testSerializeGeoLineSegmentWithAvro() throws Exception {
        LineSegment lineSegment = new LineSegment(
                new Coordinate(0, 0), new Coordinate(1, 0));
        System.out.println("-------------- Serializing JTS Model LineSegment via Avro-------------------------");
        AvroSerializationHandler avroSerializationHandler = new AvroSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);
        try {
            avroSerializationHandler.serialize(lineSegment, output);
        } finally {
            output.close();
        }
        assertTrue(new File(Utils.TEST_FILE_LOCATION).length() > 0);
        System.out.println("Successfully Serialized....");
    }

    @Test
    public void testDeserializeGeoLineSegmentWithAvro() throws Exception {
        LineSegment lineSegment = new LineSegment(
                new Coordinate(0, 0), new Coordinate(1, 0));
        AvroSerializationHandler avroSerializationHandler = new AvroSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);
        try {
            avroSerializationHandler.serialize(lineSegment, output);
            System.out.println("-------------- Deserializing JTS Model LineSegment via Avro -------------------------");
            AvroDeserializationHandler avroDeserializationHandler = new AvroDeserializationHandler();
            LineSegment lineSegmentDeserialized = avroDeserializationHandler.deserializeLine(new FileInputStream(Utils.TEST_FILE_LOCATION));
            assertEquals(lineSegment, lineSegmentDeserialized);
            System.out.println("Successfully Deserialized : " + lineSegmentDeserialized);
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
