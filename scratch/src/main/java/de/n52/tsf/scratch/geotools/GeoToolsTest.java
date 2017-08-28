package de.n52.tsf.scratch.geotools;

import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.GridFormatFinder;
import org.geotools.coverage.grid.io.imageio.IIOMetadataDumper;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.geotools.gce.geotiff.GeoTiffWriter;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.coverage.grid.GridCoordinates;
import org.opengis.coverage.grid.GridEnvelope;
import org.opengis.referencing.operation.TransformException;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;


public class GeoToolsTest {


    public static void main (String[] args) throws IOException, TransformException {
        ClassLoader classLoader = GeoToolsTest.class.getClassLoader();

        //source geo.tiff - https://github.com/geotools/geotools/tree/master/modules/plugin/geotiff/src/test/resources/org/geotools/gce/geotiff/test-data
        File file = new File(classLoader.getResource("geotif/geo.tiff").getFile());
        extractPoints(file);
        createGeotif(10,10,1);
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

        System.out.println(coverage.getRenderedImage().getColorModel().getColorSpace().getType());

        for (int i = 0; i < w; i++) {
            for (int j = 0; j < h; j++) {

                org.geotools.geometry.Envelope2D pixelEnvelop =
                        geometry.gridToWorld(new GridEnvelope2D(i, j, 1, 1));

                double latitude = pixelEnvelop.getCenterX();
                double longitude = pixelEnvelop.getCenterY();

                System.out.println(latitude + ":" + longitude);
            }
        }
    }

    public static void createGeotif(int x, int y, int c) throws IOException {
        ClassLoader classLoader = GeoToolsTest.class.getClassLoader();

        //source geo.tiff - https://github.com/geotools/geotools/tree/master/modules/plugin/geotiff/src/test/resources/org/geotools/gce/geotiff/test-data
        File file = new File(classLoader.getResource("geotif/test.tif").getFile());

        // write down a fake geotiff with non-standard CRS
        GridCoverageFactory factory = new GridCoverageFactory();
        BufferedImage bi = new BufferedImage(x, y, c);
        ReferencedEnvelope envelope = new ReferencedEnvelope(0, x, 0, y, DefaultGeographicCRS.WGS84);
        GridCoverage2D test = factory.create("test", bi, envelope);
        GeoTiffWriter writer = new GeoTiffWriter(file);
        writer.write(test, null);
        writer.dispose();

        // read
        final GeoTiffReader reader = new GeoTiffReader(file);
        System.out.println(reader.getCrs());
    }
}