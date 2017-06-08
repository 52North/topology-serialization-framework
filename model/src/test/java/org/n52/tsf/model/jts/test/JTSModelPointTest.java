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
import org.locationtech.jts.geom.Point;
import org.n52.tsf.model.jts.PBSerializationHandler;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertTrue;

public class JTSModelPointTest {
    private static final String TEST_FILE_LOCATION = "src/test/resources/jts_data";

    @Before
    public void setUp() throws Exception {
        Path filePath = Paths.get(TEST_FILE_LOCATION);
        Files.createFile(filePath);
    }

    @Test
    public void testSerializeGeoPoint() throws Exception {
        GeometryFactory geometryFactory = new GeometryFactory();
        Coordinate coordinate = new Coordinate(1,2);
        Point point = geometryFactory.createPoint(coordinate);
        System.out.println("-------------- Serializing JTS Model Point via Protobuf -------------------------");
        PBSerializationHandler pbSerializer = new PBSerializationHandler();
        FileOutputStream output = new FileOutputStream(TEST_FILE_LOCATION);
        try {
            pbSerializer.serialize(point,output);
            System.out.println("Successfully Serialized....");
        } finally {
            output.close();
        }
        assertTrue(new File(TEST_FILE_LOCATION).length() > 0);
    }

    @After
    public void tearDown() throws Exception {
        Path filePath = Paths.get(TEST_FILE_LOCATION);
        Files.deleteIfExists(filePath);
    }
}