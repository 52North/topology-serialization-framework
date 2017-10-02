package org.n52.tsf.samples.benchmark;


import org.apache.commons.cli.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import org.geotools.gml3.GMLConfiguration;
import org.geotools.xml.Parser;
import org.geotools.xml.StreamingParser;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

public class GMLVectorDataBenchmark {

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
            gmlSerializationForShpFilesCase1(fileInput);
        } catch (ParseException parseException) {
            System.out.println(
                    "ERROR: Unable to parse command-line arguments " + Arrays.toString(args) + " due to: "
                            + parseException);
        }
    }

    private static void gmlSerializationForShpFilesCase1(File input) throws IOException {
        System.out.println("-------------- Serializing / Deserializing with gml ------------------------");
        String fileName = FilenameUtils.getBaseName(input.getName());
        String outputFile = input.getParent() + File.separator + fileName;
        Files.createFile(Paths.get(outputFile));
        long timeSpentDe = 0;
        for (int i = 0; i < 50; i++) {
            InputStream in = new FileInputStream(input);
            GMLConfiguration gml = new GMLConfiguration();
            Parser parser = new Parser(gml);
            parser.setStrict(false);
            long startTime1 = System.nanoTime();
            try {
                parser.parse(in);
            } catch (SAXException e) {
                e.printStackTrace();
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            }
            long endTime1 = System.nanoTime();
            timeSpentDe = timeSpentDe + (endTime1 - startTime1);
        }
        System.out.println("TimeDe : " + timeSpentDe / (50 * 1000 * 1000) + " ms");
    }
}
