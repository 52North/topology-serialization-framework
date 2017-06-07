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
package org.n52.tsf.serialization.protobuf.test;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.n52.tsf.serialization.protobuf.gen.GeoProtobuf.Coordinate;
import org.n52.tsf.serialization.protobuf.gen.GeoProtobuf.Geometry;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class GoPointTest {
    private static final String TEST_FILE_LOCATION = "src/test/resources/geo_data";

    @Before
    public void setUp() throws Exception {
        Path filePath = Paths.get(TEST_FILE_LOCATION);
        Files.createFile(filePath);
    }

    @Test
    public void testSerializeGeoPoint() throws Exception {
        Coordinate.Builder cord = Coordinate.newBuilder();
        cord.setX(1);
        cord.setY(2);
        cord.setZ(Double.NaN);
        Geometry.Builder geo = Geometry.newBuilder();
        geo.setType(Geometry.Type.POINT);
        geo.addCoordinates(cord.build());
        System.out.println("-------------- Serializing Geometry Point -------------------------");
        FileOutputStream output = new FileOutputStream(TEST_FILE_LOCATION);
        try {
            geo.build().writeTo(output);
            System.out.println("Successfully Serialized....");
        } finally {
            output.close();
        }
        assertTrue(new File(TEST_FILE_LOCATION).length() > 0);
        System.out.println("-------------- DeSerializing Geometry Point -----------------------");
        Geometry geometry = Geometry.parseFrom(new FileInputStream(TEST_FILE_LOCATION));
        assertEquals(Geometry.Type.POINT, geometry.getType());
        System.out.println(geometry.getType() + "(" + geometry.getCoordinates(0).getX() + "," +
                geometry.getCoordinates(0).getY() + "," + geometry.getCoordinates(0).getZ() + ")");
        System.out.println("Successfully DeSerialized....");
    }

    @After
    public void tearDown() throws Exception {
        Path filePath = Paths.get(TEST_FILE_LOCATION);
        Files.deleteIfExists(filePath);
    }
}