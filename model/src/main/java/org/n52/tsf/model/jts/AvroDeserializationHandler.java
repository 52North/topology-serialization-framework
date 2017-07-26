package org.n52.tsf.model.jts;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.log4j.Logger;
import org.locationtech.jts.geom.*;
import org.n52.tsf.serialization.protobuf.gen.GeoProtobuf;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class provides the  Avro to JTS deserialization functionality.
 */
public class AvroDeserializationHandler {
    final static Logger logger = Logger.getLogger(AvroDeserializationHandler.class);
    GeometryFactory geometryFactory;

    public AvroDeserializationHandler() {
        this.geometryFactory = new GeometryFactory();
    }

    public Geometry deserialize(InputStream inputStream) throws IOException {
        DatumReader<org.n52.tsf.serialization.avro.gen.Geometry> datumReader =
                new SpecificDatumReader<>(org.n52.tsf.serialization.avro.gen.Geometry.class);
        DataFileStream<org.n52.tsf.serialization.avro.gen.Geometry> dataFileReader = new DataFileStream<>(inputStream, datumReader);
        org.n52.tsf.serialization.avro.gen.Geometry avroGeometry = null;
        if (dataFileReader.hasNext()) {
            avroGeometry = dataFileReader.next();
        }

        Geometry jtsGeometry = null;
        switch (avroGeometry.getType()) {
            case POINT:
                jtsGeometry = deserializePoint(avroGeometry);
                break;
            default:
                logger.error("Unsupported Geometric type");
        }
        return jtsGeometry;
    }

    private Point deserializePoint(org.n52.tsf.serialization.avro.gen.Geometry avroGeometry) {
        List<org.n52.tsf.serialization.avro.gen.Coordinate> coordinates = avroGeometry.getCoordinates();
        Coordinate jtsCoordinate = createJtsCoordinate(coordinates.get(0));
        Point point = geometryFactory.createPoint(jtsCoordinate);
        return point;
    }

    private Coordinate createJtsCoordinate(org.n52.tsf.serialization.avro.gen.Coordinate avroCoordinate) {
        Coordinate jtsCoordinate = new Coordinate(avroCoordinate.getX(), avroCoordinate.getY());
        return jtsCoordinate;
    }
}