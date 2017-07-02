package de.n52.tsf.scratch.geotools;

import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.GridFormatFinder;
import org.opengis.coverage.grid.GridCoordinates;
import org.opengis.coverage.grid.GridEnvelope;
import org.opengis.referencing.operation.TransformException;

import java.io.File;
import java.io.IOException;


public class GeoToolsTest {


    public static void main (String[] args) throws IOException, TransformException {
        ClassLoader classLoader = GeoToolsTest.class.getClassLoader();

        //source geo.tiff - https://github.com/geotools/geotools/tree/master/modules/plugin/geotiff/src/test/resources/org/geotools/gce/geotiff/test-data
        File file = new File(classLoader.getResource("geotif/no_crs_no_envelope2.tif").getFile());
        extractPoints(file);
    }

    public static void extractPoints(File file) throws TransformException, IOException {
        AbstractGridFormat format = GridFormatFinder.findFormat(file);
        GridCoverage2DReader reader = format.getReader(file);

        GridEnvelope dimensions = reader.getOriginalGridRange();
        GridCoordinates maxDimensions = dimensions.getHigh();
        int w = maxDimensions.getCoordinateValue(0) + 1;
        int h = maxDimensions.getCoordinateValue(1) + 1;
        System.out.println(maxDimensions.getCoordinateValue(0) + ":" + maxDimensions.getCoordinateValue(1));

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