package de.n52.tsf.scratch.geotools;

import org.geotools.coverage.grid.*;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.GridFormatFinder;
import org.opengis.coverage.grid.*;
import org.opengis.referencing.operation.TransformException;

import java.io.File;
import java.io.IOException;


public class GeoToolsTest {

    public void extractPoints(File file) throws TransformException, IOException {
        AbstractGridFormat format = GridFormatFinder.findFormat(file);
        GridCoverage2DReader reader = format.getReader(file);

        GridEnvelope dimensions = reader.getOriginalGridRange();
        GridCoordinates maxDimensions = dimensions.getHigh();
        int w = maxDimensions.getCoordinateValue(0) + 1;
        int h = maxDimensions.getCoordinateValue(1) + 1;

        GridCoverage2D coverage = reader.read(null);
        GridGeometry2D geometry = coverage.getGridGeometry();

        for (int i = 0; i < w; i++) {
            for (int j = 0; j < h; j++) {

                org.geotools.geometry.Envelope2D pixelEnvelop =
                        geometry.gridToWorld(new GridEnvelope2D(i, j, 1, 1));

                double latitude = pixelEnvelop.getCenterY();
                double longitude = pixelEnvelop.getCenterX();

                System.out.println(latitude + ":" + longitude);


            }
        }

    }
}