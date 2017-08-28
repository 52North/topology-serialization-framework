package org.n52.tsf.samples.benchmark;

import org.apache.commons.cli.*;
import org.apache.commons.io.FilenameUtils;
import org.geotools.feature.FeatureCollection;
import org.geotools.geojson.feature.FeatureJSON;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Benchmark test for GeoJson Serialization / Deserialization
 * To execute from command line use option -input to point to .geojson file
 */

public class GeoJsonVectorDataBenchmark {

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
            geoJsonSerializationForShpFilesCase1(fileInput);
        } catch (ParseException parseException) {
            System.out.println(
                    "ERROR: Unable to parse command-line arguments " + Arrays.toString(args) + " due to: "
                            + parseException);
        }
    }

    private static void geoJsonSerializationForShpFilesCase1(File input) throws IOException {
        System.out.println("-------------- Serializing / Deserializing with GeoJson ------------------------");
        long timeSpentSe = 0;
        long timeSpentDe = 0;
        String fileName = FilenameUtils.getBaseName(input.getName());
        String outputFile = input.getParent() + File.separator + fileName;
        Files.createFile(Paths.get(outputFile));
        FeatureJSON g = new FeatureJSON();
        FeatureCollection featureCollection = g.readFeatureCollection(new FileInputStream(input));

        for (int i = 0; i < 50; i++) {
            long startTime1 = System.nanoTime();
            g.readFeatureCollection(new FileInputStream(input));
            long endTime1 = System.nanoTime();
            timeSpentDe = timeSpentDe + (endTime1 - startTime1);
        }

        for (int i = 0; i < 50; i++) {
            long startTime2 = System.nanoTime();
            g.writeFeatureCollection(featureCollection, new FileOutputStream(outputFile));
            long endTime2 = System.nanoTime();
            timeSpentSe = timeSpentSe + (endTime2 - startTime2);
        }

        System.out.println("TimeSe : " + timeSpentSe / (50 * 1000 * 1000) + " ms");
        System.out.println("TimeDe : " + timeSpentDe / (50 * 1000 * 1000) + " ms");
    }
}
