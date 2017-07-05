package org.n52.tsf.model.gt;

import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.GridFormatFinder;
import org.geotools.data.WorldFileReader;
import org.geotools.referencing.CRS;
import org.n52.tsf.serialization.protobuf.gen.GeoProtobufCov;
import org.opengis.coverage.grid.GridCoordinates;
import org.opengis.coverage.grid.GridEnvelope;
import org.opengis.referencing.FactoryException;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * This class provides the Geotools coverage data to ProtoBuf Serialization functionality.
 */
public class PBCovDataSerializationHadler {

    public void serialize(File geotifFile, File tfwfile, OutputStream outputStream) throws Exception {
        GeoProtobufCov.Grid.Builder gridBuilder = GeoProtobufCov.Grid.newBuilder();
        setCoverageGridData(gridBuilder, geotifFile);
        setMetaData(gridBuilder, tfwfile);
        gridBuilder.build().writeTo(outputStream);
    }

    public void setMetaData(GeoProtobufCov.Grid.Builder gridBuilder, File tfwFile) throws IOException {
        WorldFileReader worldFileReader = new WorldFileReader(tfwFile);
        gridBuilder.setXulc(worldFileReader.getXULC());
        gridBuilder.setYulc(worldFileReader.getYULC());
        gridBuilder.setXPixelSize(worldFileReader.getXPixelSize());
        gridBuilder.setYPixelSize(worldFileReader.getYPixelSize());
        gridBuilder.setXRotation(worldFileReader.getRotationX());
        gridBuilder.setYRotation(worldFileReader.getRotationY());
    }

    public void setCoverageGridData(GeoProtobufCov.Grid.Builder gridBuilder, File geotifFile) throws IOException, FactoryException {
        AbstractGridFormat format = GridFormatFinder.findFormat(geotifFile);
        GridCoverage2DReader reader = format.getReader(geotifFile);
        GridEnvelope dimensions = reader.getOriginalGridRange();
        GridCoordinates maxDimensions = dimensions.getHigh();
        GridCoverage2D coverage = reader.read(null);
        gridBuilder.setMaxWidth(maxDimensions.getCoordinateValue(0) + 1);
        gridBuilder.setMaxHight(maxDimensions.getCoordinateValue(1) + 1);
        gridBuilder.setSourceCrs(CRS.lookupIdentifier(coverage.getCoordinateReferenceSystem(), true));
    }
}
