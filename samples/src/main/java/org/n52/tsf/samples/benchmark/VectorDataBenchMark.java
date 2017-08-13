package org.n52.tsf.samples.benchmark;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.shapefile.shp.ShapefileReader;
import org.n52.tsf.model.vector.jts.vividsolutions.AvroSerializationHandler;
import org.n52.tsf.model.vector.jts.vividsolutions.PBSerializationHandler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class VectorDataBenchMark {
    private static final String FILE_OUTPUT = "output";
    private static final String FILE_INPUT = "input";


    public static void main(String[] args) throws IOException {
        CommandLine line;
        Options options = new Options();
        setupOptions(options);
        CommandLineParser parser = new DefaultParser();
        try
        {
            line = parser.parse(options, args);
            File fileInput = new File(line.getOptionValue(FILE_INPUT));
            File fileOutput = null;
            if (line.hasOption(FILE_OUTPUT)) {
                fileOutput = new File(line.getOptionValue(FILE_OUTPUT));
            }
            avroSerializationForShpFiles(fileInput, fileOutput);
            pbSerializationForShpFiles(fileInput, fileOutput);
        }
        catch (ParseException parseException)
        {
            System.out.println(
                    "ERROR: Unable to parse command-line arguments " + Arrays.toString(args) + " due to: "
                            + parseException);
        }
    }

    private static void setupOptions(Options options) {
        Option input = Option.builder("i")
                .required(true)
                .argName(FILE_INPUT)
                .desc("Shape file input")
                .longOpt("input")
                .hasArg(true)
                .numberOfArgs(1)
                .type(String.class)
                .build();

        Option output = Option.builder("o")
                .required(false)
                .argName(FILE_OUTPUT)
                .desc("Serialize output file")
                .longOpt("output")
                .hasArg(true)
                .numberOfArgs(1)
                .type(String.class)
                .build();

        options.addOption(input);
        options.addOption(output);
    }

    public static void avroSerializationForShpFiles(File input, File output) throws IOException {
        List<File> files = (List<File>) FileUtils.listFiles(input,  new String[] { "shp" }, true);
        Path path;
        if(output == null) {
            path = Paths.get(input.getPath() + File.separator + "avro");
            FileUtils.deleteDirectory(path.toFile());
            Files.createDirectory(path);
        }else {
            path = output.toPath();
        }

        System.out.println("-------------- Serializing Shape files with Avro ----------------------------");
        long sourceFileSize = 0;
        long serializedFileSize = 0;
        long startTime = 0;
        long endTime = 0;
        for (File file: files){
            sourceFileSize = sourceFileSize + FileUtils.sizeOf(file);
            String fileName =  FilenameUtils.getBaseName(file.getName());
            String outputFile = path + File.separator + fileName;
            Files.createFile(Paths.get(outputFile));
            AvroSerializationHandler avroSerializationHandlervs =new AvroSerializationHandler();
            GeometryCollection geoCollection = getGeometryCollection(file);
            startTime = System.nanoTime();
            avroSerializationHandlervs.serialize(geoCollection, new FileOutputStream(outputFile));
            endTime = System.nanoTime();
            serializedFileSize = serializedFileSize + FileUtils.sizeOf(new File(outputFile));
        }
        System.out.println("Source File size : " + (sourceFileSize/(1024 * 1024)) + " mb" );
        System.out.println("Serialized File size : " + (serializedFileSize/(1024 * 1024)) + " mb" );
        System.out.println("Time : " + (endTime - startTime)/1000 + " μs");
    }

    public static void pbSerializationForShpFiles(File input, File output) throws IOException {
        List<File> files = (List<File>) FileUtils.listFiles(input,  new String[] { "shp" }, true);
        Path path;
        if(output == null) {
            path = Paths.get(input.getPath() + File.separator + "pb");
            FileUtils.deleteDirectory(path.toFile());
            Files.createDirectory(path);
        }else {
            path = output.toPath();
        }
        System.out.println("-------------- Serializing Shape files with Protobuf ------------------------");
        long sourceFileSize = 0;
        long serializedFileSize = 0;
        long startTime = 0;
        long endTime = 0;
        for (File file: files){
            sourceFileSize = sourceFileSize + FileUtils.sizeOf(file);
            String fileName =  FilenameUtils.getBaseName(file.getName());
            String outputFile = path + File.separator + fileName;
            Files.createFile(Paths.get(outputFile));
            PBSerializationHandler pbSerializationHandlervs =new PBSerializationHandler();
            GeometryCollection geoCollection = getGeometryCollection(file);
            startTime = System.nanoTime();
            pbSerializationHandlervs.serialize(geoCollection, new FileOutputStream(outputFile));
            endTime = System.nanoTime();
            serializedFileSize = serializedFileSize + FileUtils.sizeOf(new File(outputFile));
        }
        System.out.println("Source File size : " + (sourceFileSize/(1024 * 1024)) + " mb" );
        System.out.println("Serialized File size : " + (serializedFileSize/(1024 * 1024)) + " mb" );
        System.out.println("Time : " + (endTime - startTime)/1000 + " μs");
    }

    private static GeometryCollection getGeometryCollection(File shapeFile) throws IOException {
        ShpFiles shpFiles = new ShpFiles(shapeFile.toURI().toURL());
        ShapefileReader reader = new ShapefileReader(shpFiles, false, false, new GeometryFactory());
        List<Geometry> geometries = new ArrayList<>();
        while (reader.hasNext()) {
            ShapefileReader.Record record = reader.nextRecord();
            geometries.add ((Geometry) record.shape());
        }
        reader.close();
        return new GeometryFactory().createGeometryCollection(geometries.toArray(new Geometry[geometries.size()]));
    }
}
