package de.n52.tsf.scratch.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.n52.tsf.serialization.avro.test.gen.Coordinate;

import java.io.File;
import java.io.IOException;

import static java.lang.Double.NaN;

public class AvroSerializationSample {

    public static void main(String[] args) throws IOException {
        ClassLoader classLoader = AvroSerializationSample.class.getClassLoader();
        File schemaFile = new File(classLoader.getResource("avro/geocoordinate.avsc").getFile());
        File tmp = new File(classLoader.getResource("avro/tmp.avro").getFile());


        Coordinate coordinate = Coordinate.newBuilder().setX(1.0).setY(2.0).setZ(NaN).build();

        //serializeWithCodeGen
        serializeWithCodeGen(coordinate, tmp);
        //deserializeWithCodeGen
        deserializeWithCodeGen(tmp);

        Schema schema = new Schema.Parser().parse(schemaFile);
        GenericRecord coordinate1 = new GenericData.Record(schema);
        coordinate1.put("x", 1.0);
        coordinate1.put("y", 2.0);
        coordinate1.put("z", NaN);

        //deserializeWithoutCodeGen
        serializeWithoutCodeGen(schema, coordinate1, tmp);
        //deserializeWithoutCodeGen
        deserializeWithoutCodeGen(schema, tmp);

    }

    public static void serializeWithCodeGen(Coordinate coordinate, File file) throws IOException {
        DatumWriter<Coordinate> datumWriter = new SpecificDatumWriter<>(Coordinate.class);
        DataFileWriter<Coordinate> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(coordinate.getSchema(), file);
        dataFileWriter.append(coordinate);
        dataFileWriter.close();
    }

    public static void deserializeWithCodeGen(File file) throws IOException {
        DatumReader<Coordinate> datumReader = new SpecificDatumReader<>(Coordinate.class);
        DataFileReader<Coordinate> dataFileReader = new DataFileReader<>(file, datumReader);
        Coordinate coordinate = null;
        while (dataFileReader.hasNext()) {
            coordinate = dataFileReader.next(coordinate);
            System.out.println(coordinate);
        }
    }

    public static void serializeWithoutCodeGen(Schema schema, GenericRecord coordinate, File file) throws IOException {
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(coordinate);
        dataFileWriter.close();
    }

    public static void deserializeWithoutCodeGen(Schema schema, File file) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader);
        GenericRecord coordinate = null;
        while (dataFileReader.hasNext()) {
            coordinate = dataFileReader.next(coordinate);
            System.out.println(coordinate);
        }
    }
}
