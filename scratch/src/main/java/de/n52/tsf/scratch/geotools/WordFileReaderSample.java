package de.n52.tsf.scratch.geotools;

import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.data.WorldFileReader;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.matrix.GeneralMatrix;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.opengis.coverage.grid.GridCoordinates;
import org.opengis.coverage.grid.GridEnvelope;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

public class WordFileReaderSample {

    public static void main(String[] args) throws IOException {
        ClassLoader classLoader = WordFileReaderSample.class.getClassLoader();

        //source no_crs_no_envelope2.tfw - https://github.com/geotools/geotools/tree/master/modules/plugin/geotiff/src/test/resources/org/geotools/gce/geotiff/test-data
        File file = new File(classLoader.getResource("geotif/no_crs_no_envelope2.tfw").getFile());
        WorldFileReader worldFileReader = new WorldFileReader(file);
        System.out.println(worldFileReader.getXULC());
        System.out.println(worldFileReader.getYULC());
        System.out.println(worldFileReader.getXPixelSize());
        System.out.println(worldFileReader.getYPixelSize());
        System.out.println(worldFileReader.getRotationX());
        System.out.println(worldFileReader.getRotationY());

        MathTransform mt = createMathtransform(worldFileReader);
        System.out.println(mt.toWKT());
    }

    public void tfwtocovmodel(File tfwFile) throws IOException {
        int w = 12;
        int h = 12;
        GridCoverageFactory factory = new GridCoverageFactory();
        BufferedImage image = new BufferedImage(w, h, 1);
        ReferencedEnvelope envelope = new ReferencedEnvelope(0, w, 0, h, null);

        GridCoverage2D geometry = factory.create("test", image, envelope);
        WorldFileReader reader = new WorldFileReader(tfwFile);
        MathTransform raster2Model = createMathtransform(reader);

        for (int i = 0; i < w; i++) {
            for (int j = 0; j < h; j++) {
                GeneralEnvelope ge = new GeneralEnvelope(new GridEnvelope2D(i, j, 1, 1), PixelInCell.CELL_CENTER, raster2Model, null);
                Envelope2D pixelEnvelop = new Envelope2D(ge);
                double latitude = pixelEnvelop.getCenterX();
                double longitude = pixelEnvelop.getCenterY();
                System.out.println(latitude + ":" + longitude);

            }
        }
    }

    public static MathTransform createMathtransform(WorldFileReader worldFileReader){
        GeneralMatrix gm = new GeneralMatrix(3);
        gm.setElement(0, 0, worldFileReader.getXPixelSize());
        gm.setElement(1, 1, worldFileReader.getYPixelSize());
        gm.setElement(0, 1, worldFileReader.getRotationX());
        gm.setElement(1, 0, worldFileReader.getRotationY());
        gm.setElement(0, 2, worldFileReader.getXULC());
        gm.setElement(1, 2, worldFileReader.getYULC());
        return ProjectiveTransform.create(gm);
    }
}
