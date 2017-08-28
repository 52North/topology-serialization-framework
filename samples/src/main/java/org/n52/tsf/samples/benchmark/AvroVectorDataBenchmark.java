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

package org.n52.tsf.samples.benchmark;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.shapefile.shp.ShapefileReader;
import org.n52.tsf.model.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Benchmark test for Avro Serialization / Deserialization
 * To execute from command line use option -input to point to .shp file
 */

public class AvroVectorDataBenchmark {

    public static void main(String[] args) throws IOException {
        CommandLine line;
        Options options = new Options();
        BenchMarkUtils.setupOptions(options);
        CommandLineParser parser = new DefaultParser();
        try {
            line = parser.parse(options, args);
            File fileInput = new File(line.getOptionValue(BenchMarkUtils.FILE_INPUT));
            File fileOutput = null;
            if (line.hasOption(BenchMarkUtils.FILE_OUTPUT)) {
                fileOutput = new File(line.getOptionValue(BenchMarkUtils.FILE_OUTPUT));
            }
            avroSerializationForShpFilesCase1(fileInput);
        } catch (ParseException parseException) {
            System.out.println(
                    "ERROR: Unable to parse command-line arguments " + Arrays.toString(args) + " due to: "
                            + parseException);
        }
    }

    private static void avroSerializationForShpFilesCase1(File input) throws IOException {

        System.out.println("-------------- Serializing / Deserializing Shape files with Avro ------------------------");
        long timeSpentSe = 0;
        long timeSpentDe = 0;
        long sourceFileSize = FileUtils.sizeOf(input);
        String fileName = FilenameUtils.getBaseName(input.getName());
        String outputFile = input.getParent() + File.separator + fileName;
        Files.createFile(Paths.get(outputFile));
        GeometryCollection geoCollection = BenchMarkUtils.getGeometryCollection(input);
        for (int i = 0; i < 50; i++) {
            SerializationHandler avroSerializer = SerializationFactory.createSerializer(new FileOutputStream(outputFile), SerializerType.AVRO_SERIALIZER_VS);
            long startTime1 = System.nanoTime();
            avroSerializer.serialize(geoCollection);
            long endTime1 = System.nanoTime();
            timeSpentSe = timeSpentSe + (endTime1 - startTime1);
            avroSerializer.close();
        }
        long serializedFileSize = FileUtils.sizeOf(new File(outputFile));

        for (int i = 0; i < 50; i++) {
            DeserializationHandler avroDeserializationHandler = DeserializationFactory.createDeserializer(new FileInputStream(outputFile), DeserializerType.AVRO_DESERIALIZER_VS);
            long startTime2 = System.nanoTime();
            GeometryCollection geometryCollection = (GeometryCollection) avroDeserializationHandler.deserialize();
            long endTime2 = System.nanoTime();
            timeSpentDe = timeSpentDe + (endTime2 - startTime2);
            avroDeserializationHandler.close();
        }

        System.out.println("Source File size : " + (sourceFileSize / (1024 * 1024)) + " mb");
        System.out.println("Serialized File size : " + (serializedFileSize / (1024 * 1024)) + " mb");
        System.out.println("TimeSe : " + timeSpentSe / (50 * 1000 * 1000) + " ms");
        System.out.println("TimeDe : " + timeSpentDe / (50 * 1000 * 1000) + " ms");
    }

    /*
     * Using Streaming approach for serialization
     */
    public static void avroSerializationForShpFilesCase2(File input, File output) throws IOException {
        List<File> files = (List<File>) FileUtils.listFiles(input, new String[]{"shp"}, true);
        Path path;
        if (output == null) {
            path = Paths.get(input.getPath() + File.separator + "avro");
            FileUtils.deleteDirectory(path.toFile());
            Files.createDirectory(path);
        } else {
            path = output.toPath();
        }

        System.out.println("-------------- Serializing / Deserializing Shape files with Avro ----------------------------");
        long sourceFileSize = 0;
        long serializedFileSize = 0;
        long timeSpent = 0;
        for (File file : files) {
            sourceFileSize = sourceFileSize + FileUtils.sizeOf(file);
            String fileName = FilenameUtils.getBaseName(file.getName());
            String outputFile = path + File.separator + fileName;
            Files.createFile(Paths.get(outputFile));
            SerializationHandler avroSerializer = SerializationFactory.createSerializer(new FileOutputStream(outputFile), SerializerType.AVRO_SERIALIZER_VS);
            ShpFiles shpFiles = new ShpFiles(file.toURI().toURL());
            ShapefileReader reader = new ShapefileReader(shpFiles, false, false, new GeometryFactory());
            long time = 0;
            while (reader.hasNext()) {
                ShapefileReader.Record record = reader.nextRecord();
                long startTime = System.nanoTime();
                avroSerializer.serialize((Geometry) record.shape());
                long endTime = System.nanoTime();
                time = time + (endTime - startTime);
            }
            reader.close();
            avroSerializer.close();
            timeSpent = timeSpent + time;
            serializedFileSize = serializedFileSize + FileUtils.sizeOf(new File(outputFile));
        }
        System.out.println("Source File size : " + (sourceFileSize / (1024 * 1024)) + " mb");
        System.out.println("Serialized File size : " + (serializedFileSize / (1024 * 1024)) + " mb");
        System.out.println("Time : " + timeSpent / 1000 + " μs");
    }
}
