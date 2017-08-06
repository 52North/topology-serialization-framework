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

public class JTSModelPolygonTest {

    @Before
    public void setUp() throws Exception {
        Path filePath = Paths.get(Utils.TEST_FILE_LOCATION);
        Files.createFile(filePath);
    }

    @Test
    public void serializeGeoPolygonTestA() throws Exception {
        GeometryFactory geometryFactory = new GeometryFactory();
        Polygon polygon = geometryFactory.createPolygon(new Coordinate[]{
                new Coordinate(0, 0), new Coordinate(10, 0), new Coordinate(0, 10), new Coordinate(10, 10), new Coordinate(0, 0)});
        System.out.println("-------------- Serializing JTS Model Polygon without holes via Protobuf -------------------------");
        PBSerializationHandler pbSerializer = new PBSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);
        try {
            pbSerializer.serialize(polygon, output);
            System.out.println("Successfully Serialized....");
        } finally {
            output.close();
        }
        assertTrue(new File(Utils.TEST_FILE_LOCATION).length() > 0);
    }

    @Test
    public void serializeGeoPolygonTestB() throws Exception {
        GeometryFactory geometryFactory = new GeometryFactory();
        LinearRing externalLR = geometryFactory.createLinearRing(new Coordinate[]{
                new Coordinate(0, 0), new Coordinate(10, 0), new Coordinate(0, 10), new Coordinate(10, 10), new Coordinate(0, 0)});
        LinearRing[] internalLRs = new LinearRing[]{geometryFactory.createLinearRing(new Coordinate[]{
                new Coordinate(2, 2), new Coordinate(2, 3), new Coordinate(3, 3), new Coordinate(3, 2), new Coordinate(2, 2)})};
        Polygon polygon = geometryFactory.createPolygon(externalLR, internalLRs);
        System.out.println("----------------- Serializing JTS Model Polygon with holes via Protobuf -------------------------");
        PBSerializationHandler pbSerializer = new PBSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);
        try {
            pbSerializer.serialize(polygon, output);
            System.out.println("Successfully Serialized....");
        } finally {
            output.close();
        }
        assertTrue(new File(Utils.TEST_FILE_LOCATION).length() > 0);
    }

        @Test
    public void deserializeGeoPolygonTestC() throws Exception {
        GeometryFactory geometryFactory = new GeometryFactory();
        Polygon polygon = geometryFactory.createPolygon(new Coordinate[]{
                new Coordinate(0, 0), new Coordinate(10, 0), new Coordinate(0, 10),  new Coordinate(10, 10),  new Coordinate(0, 0)});
        PBSerializationHandler pbSerializer = new PBSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);
        try {
            pbSerializer.serialize(polygon,output);
            System.out.println("-------------- Deserializing JTS Model Polygon without holes via Protobuf -------------------------");
            PBDeserializationHandler pbDeserializationHandler = new PBDeserializationHandler();
            Polygon polygonDeserialized = (Polygon) pbDeserializationHandler.deserialize(new FileInputStream(Utils.TEST_FILE_LOCATION));
            assertEquals(polygon, polygonDeserialized);
            System.out.println("Successfully Deserialized : " + polygonDeserialized);
        } finally {
            output.close();
        }
    }

    @Test
    public void deserializeGeoPolygonTestD() throws Exception {
        GeometryFactory geometryFactory = new GeometryFactory();
        LinearRing externalLR = geometryFactory.createLinearRing(new Coordinate[]{
                new Coordinate(0, 0), new Coordinate(10, 0), new Coordinate(0, 10),  new Coordinate(10, 10),  new Coordinate(0, 0)});
        LinearRing[] internalLRs = new LinearRing[]{geometryFactory.createLinearRing(new Coordinate[]{
                new Coordinate(2, 2), new Coordinate(2, 3), new Coordinate(3, 3),  new Coordinate(3, 2),  new Coordinate(2, 2)})};
        Polygon polygon = geometryFactory.createPolygon(externalLR, internalLRs);
        PBSerializationHandler pbSerializer = new PBSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);
        try {
            pbSerializer.serialize(polygon,output);
            System.out.println("----------------- Deserializing JTS Model Polygon with holes via Protobuf -------------------------");
            PBDeserializationHandler pbDeserializationHandler = new PBDeserializationHandler();
            Polygon polygonSerialized = (Polygon) pbDeserializationHandler.deserialize(new FileInputStream(Utils.TEST_FILE_LOCATION));
            assertEquals(polygon, polygonSerialized);
            System.out.println("Successfully Deserialized : " + polygonSerialized);
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
