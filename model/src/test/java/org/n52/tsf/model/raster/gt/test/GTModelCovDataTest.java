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

package org.n52.tsf.model.gt.test;

import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.referencing.CRS;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.n52.tsf.model.gt.AvroCovDataDeserializationHandler;
import org.n52.tsf.model.gt.AvroCovDataSerializationHandler;
import org.n52.tsf.model.gt.PBCovDataDeserializationHandler;
import org.n52.tsf.model.gt.PBCovDataSerializationHandler;
import org.n52.tsf.model.gt.beans.GeoPoint;
import org.n52.tsf.model.jts.test.Utils;
import org.opengis.coverage.grid.GridCoordinates;
import org.opengis.coverage.grid.GridEnvelope;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GTModelCovDataTest {

    @Before
    public void setUp() throws Exception {
        Path filePath = Paths.get(Utils.TEST_FILE_LOCATION);
        if(Files.notExists(filePath)) {
            Files.createFile(filePath);
        }
    }

    @Test
    public void testSerializeGeoTif() throws Exception {
        ClassLoader classLoader = GTModelCovDataTest.class.getClassLoader();
        //source no_crs_no_envelope2.tfw - https://github.com/geotools/geotools/tree/master/modules/plugin/geotiff/src/test/resources/org/geotools/gce/geotiff/test-data
        File tfwFile = new File(classLoader.getResource("geotif/no_crs_no_envelope2.tfw").getFile());
        File tifFile = new File(classLoader.getResource("geotif/no_crs_no_envelope2.tif").getFile());

        System.out.println("-------------- Serializing Geotif coverage Model via Protobuf -------------------------");
        PBCovDataSerializationHandler pbCovDataSerializationHandler = new PBCovDataSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);
        try {
            pbCovDataSerializationHandler.serialize(tifFile, tfwFile, output);
        } finally {
            output.close();
        }
        assertTrue(new File(Utils.TEST_FILE_LOCATION).length() > 0);
        System.out.println("Successfully Serialized....");
    }

    @Test
    public void testDeserializeGeoTif() throws Exception {
        ClassLoader classLoader = GTModelCovDataTest.class.getClassLoader();
        //source no_crs_no_envelope2.tfw - https://github.com/geotools/geotools/tree/master/modules/plugin/geotiff/src/test/resources/org/geotools/gce/geotiff/test-data
        File tfwFile = new File(classLoader.getResource("geotif/no_crs_no_envelope2.tfw").getFile());
        File tifFile = new File(classLoader.getResource("geotif/no_crs_no_envelope2.tif").getFile());
        System.out.println("-------------- Deserializing Geotif coverage Model via Protobuf -------------------------");
        PBCovDataSerializationHandler pbCovDataSerializationHandler = new PBCovDataSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);

        try {
            pbCovDataSerializationHandler.serialize(tifFile, tfwFile, output);
            PBCovDataDeserializationHandler pbCovDatadeSerializationHandler = new PBCovDataDeserializationHandler();
            GridCoverage2D gridCoverage = pbCovDatadeSerializationHandler.deserialize("testgeotif",new FileInputStream(Utils.TEST_FILE_LOCATION));
            GridEnvelope dimensions = gridCoverage.getGridGeometry().getGridRange();
            GridCoordinates maxDimensions = dimensions.getHigh();
            assertEquals(maxDimensions.getCoordinateValue(0), 11);
            assertEquals(maxDimensions.getCoordinateValue(1), 11);
            assertEquals("EPSG:404000", CRS.lookupIdentifier(gridCoverage.getCoordinateReferenceSystem(), true));
        } finally {
            output.close();
        }
        System.out.println("Successfully Deserialized....");
    }

    @Test
    public void testSerializeGeoTifWithoutMetaData() throws Exception {
        ClassLoader classLoader = GTModelCovDataTest.class.getClassLoader();
        //source geo.tiff - https://github.com/geotools/geotools/tree/master/modules/plugin/geotiff/src/test/resources/org/geotools/gce/geotiff/test-data
        File tifFile = new File(classLoader.getResource("geotif/geo.tiff").getFile());

        System.out.println("-------------- Serializing Geotif coverage Model with CRS via Protobuf -------------------------");
        PBCovDataSerializationHandler pbCovDataSerializationHadler = new PBCovDataSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);
        try {
            pbCovDataSerializationHadler.serialize(tifFile, output, false);
        } finally {
            output.close();
        }
        assertTrue(new File(Utils.TEST_FILE_LOCATION).length() > 0);
        System.out.println("Successfully Serialized....");
    }

    @Test
    public void testDeserializeGeoTifWithoutMetaData() throws Exception {
        ClassLoader classLoader = GTModelCovDataTest.class.getClassLoader();
        //source geo.tiff - https://github.com/geotools/geotools/tree/master/modules/plugin/geotiff/src/test/resources/org/geotools/gce/geotiff/test-data
        File tifFile = new File(classLoader.getResource("geotif/geo.tiff").getFile());

        System.out.println("-------------- Deserializing Geotif coverage Model with CRS via Protobuf -------------------------");
        PBCovDataSerializationHandler pbCovDataSerializationHadler = new PBCovDataSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);
        try {
            pbCovDataSerializationHadler.serialize(tifFile, output, false);
            PBCovDataDeserializationHandler pbCovDatadeSerializationHandler = new PBCovDataDeserializationHandler();
            List<GeoPoint> geoPoints = pbCovDatadeSerializationHandler.deserializeToWorld(new FileInputStream(Utils.TEST_FILE_LOCATION));
            assertEquals(14400, geoPoints.size());
        } finally {
            output.close();
        }
        assertTrue(new File(Utils.TEST_FILE_LOCATION).length() > 0);
        System.out.println("Successfully Deserialized....");
    }

    @Test
    public void testDeserializeGeoTifWithMetaData() throws Exception {
        ClassLoader classLoader = GTModelCovDataTest.class.getClassLoader();
        //source geo.tiff - https://github.com/geotools/geotools/tree/master/modules/plugin/geotiff/src/test/resources/org/geotools/gce/geotiff/test-data
        File tifFile = new File(classLoader.getResource("geotif/geo.tiff").getFile());
        System.out.println("-------------- Deserializing Geotif coverage Model via Protobuf -------------------------");
        PBCovDataSerializationHandler pbCovDataSerializationHandler = new PBCovDataSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);

        try {
            pbCovDataSerializationHandler.serialize(tifFile, output, true);
            PBCovDataDeserializationHandler pbCovDatadeSerializationHandler = new PBCovDataDeserializationHandler();
            GridCoverage2D gridCoverage = pbCovDatadeSerializationHandler.deserialize("testgeotif", new FileInputStream(Utils.TEST_FILE_LOCATION));
            GridEnvelope dimensions = gridCoverage.getGridGeometry().getGridRange();
            GridCoordinates maxDimensions = dimensions.getHigh();
            assertEquals(119, maxDimensions.getCoordinateValue(0));
            assertEquals(119, maxDimensions.getCoordinateValue(1));
            assertEquals("EPSG:4301", CRS.lookupIdentifier(gridCoverage.getCoordinateReferenceSystem(), true));
        } finally {
            output.close();
        }
        System.out.println("Successfully Deserialized....");
    }

    @Test
    public void testSerializeGeoTifWithAvro() throws Exception {
        ClassLoader classLoader = GTModelCovDataTest.class.getClassLoader();
        //source no_crs_no_envelope2.tfw - https://github.com/geotools/geotools/tree/master/modules/plugin/geotiff/src/test/resources/org/geotools/gce/geotiff/test-data
        File tfwFile = new File(classLoader.getResource("geotif/no_crs_no_envelope2.tfw").getFile());
        File tifFile = new File(classLoader.getResource("geotif/no_crs_no_envelope2.tif").getFile());

        System.out.println("-------------- Serializing Geotif coverage Model via Avro -------------------------");
        AvroCovDataSerializationHandler avroCovDataSerializationHandler = new AvroCovDataSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);
        try {
            avroCovDataSerializationHandler.serialize(tifFile, tfwFile, output);
        } finally {
            output.close();
        }
        assertTrue(new File(Utils.TEST_FILE_LOCATION).length() > 0);
        System.out.println("Successfully Serialized....");
    }

    @Test
    public void testDeserializeGeoTifWithAvro() throws Exception {
        ClassLoader classLoader = GTModelCovDataTest.class.getClassLoader();
        //source no_crs_no_envelope2.tfw - https://github.com/geotools/geotools/tree/master/modules/plugin/geotiff/src/test/resources/org/geotools/gce/geotiff/test-data
        File tfwFile = new File(classLoader.getResource("geotif/no_crs_no_envelope2.tfw").getFile());
        File tifFile = new File(classLoader.getResource("geotif/no_crs_no_envelope2.tif").getFile());
        System.out.println("-------------- Deserializing Geotif coverage Model via Avro -------------------------");
        AvroCovDataSerializationHandler avroCovDataSerializationHandler = new AvroCovDataSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);

        try {
            avroCovDataSerializationHandler.serialize(tifFile, tfwFile, output);
            AvroCovDataDeserializationHandler avroCovDatadeSerializationHandler = new AvroCovDataDeserializationHandler();
            GridCoverage2D gridCoverage = avroCovDatadeSerializationHandler.deserialize("testgeotif",new FileInputStream(Utils.TEST_FILE_LOCATION));
            GridEnvelope dimensions = gridCoverage.getGridGeometry().getGridRange();
            GridCoordinates maxDimensions = dimensions.getHigh();
            assertEquals(maxDimensions.getCoordinateValue(0), 11);
            assertEquals(maxDimensions.getCoordinateValue(1), 11);
            assertEquals("EPSG:404000", CRS.lookupIdentifier(gridCoverage.getCoordinateReferenceSystem(), true));
        } finally {
            output.close();
        }
        System.out.println("Successfully Deserialized....");
    }

    @Test
    public void testSerializeGeoTifWithoutMetaDataWithAvro() throws Exception {
        ClassLoader classLoader = GTModelCovDataTest.class.getClassLoader();
        //source geo.tiff - https://github.com/geotools/geotools/tree/master/modules/plugin/geotiff/src/test/resources/org/geotools/gce/geotiff/test-data
        File tifFile = new File(classLoader.getResource("geotif/geo.tiff").getFile());

        System.out.println("-------------- Serializing Geotif coverage Model with CRS via Avro -------------------------");
        AvroCovDataSerializationHandler avroCovDataSerializationHadler = new AvroCovDataSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);
        try {
            avroCovDataSerializationHadler.serialize(tifFile, output, false);
        } finally {
            output.close();
        }
        assertTrue(new File(Utils.TEST_FILE_LOCATION).length() > 0);
        System.out.println("Successfully Serialized....");
    }

    @Test
    public void testDeserializeGeoTifWithoutMetaDataWithAvro() throws Exception {
        ClassLoader classLoader = GTModelCovDataTest.class.getClassLoader();
        //source geo.tiff - https://github.com/geotools/geotools/tree/master/modules/plugin/geotiff/src/test/resources/org/geotools/gce/geotiff/test-data
        File tifFile = new File(classLoader.getResource("geotif/geo.tiff").getFile());

        System.out.println("-------------- Deserializing Geotif coverage Model with CRS via Avro -------------------------");
        AvroCovDataSerializationHandler avroCovDataSerializationHadler = new AvroCovDataSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);
        try {
            avroCovDataSerializationHadler.serialize(tifFile, output, false);
            AvroCovDataDeserializationHandler avroCovDatadeSerializationHandler = new AvroCovDataDeserializationHandler();
            List<GeoPoint> geoPoints = avroCovDatadeSerializationHandler.deserializeToWorld(new FileInputStream(Utils.TEST_FILE_LOCATION));
            assertEquals(14400, geoPoints.size());
        } finally {
            output.close();
        }
        assertTrue(new File(Utils.TEST_FILE_LOCATION).length() > 0);
        System.out.println("Successfully Deserialized....");
    }

    @Test
    public void testDeserializeGeoTifWithMetaDataWithAvro() throws Exception {
        ClassLoader classLoader = GTModelCovDataTest.class.getClassLoader();
        //source geo.tiff - https://github.com/geotools/geotools/tree/master/modules/plugin/geotiff/src/test/resources/org/geotools/gce/geotiff/test-data
        File tifFile = new File(classLoader.getResource("geotif/geo.tiff").getFile());
        System.out.println("-------------- Deserializing Geotif coverage Model via Avro -------------------------");
       AvroCovDataSerializationHandler avroCovDataSerializationHandler = new AvroCovDataSerializationHandler();
        FileOutputStream output = new FileOutputStream(Utils.TEST_FILE_LOCATION);

        try {
            avroCovDataSerializationHandler.serialize(tifFile, output, true);
            AvroCovDataDeserializationHandler avroCovDatadeSerializationHandler = new AvroCovDataDeserializationHandler();
            GridCoverage2D gridCoverage = avroCovDatadeSerializationHandler.deserialize("testgeotif", new FileInputStream(Utils.TEST_FILE_LOCATION));
            GridEnvelope dimensions = gridCoverage.getGridGeometry().getGridRange();
            GridCoordinates maxDimensions = dimensions.getHigh();
            assertEquals(119, maxDimensions.getCoordinateValue(0));
            assertEquals(119, maxDimensions.getCoordinateValue(1));
            assertEquals("EPSG:4301", CRS.lookupIdentifier(gridCoverage.getCoordinateReferenceSystem(), true));
        } finally {
            output.close();
        }
        System.out.println("Successfully Deserialized....");
    }

    @After
    public void tearDown() throws Exception {
        Path filePath = Paths.get(Utils.TEST_FILE_LOCATION);
        Files.deleteIfExists(filePath);
    }
}