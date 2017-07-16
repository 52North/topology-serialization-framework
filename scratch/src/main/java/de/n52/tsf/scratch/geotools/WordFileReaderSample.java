package de.n52.tsf.scratch.geotools;

import org.geotools.data.WorldFileReader;
import org.geotools.referencing.operation.matrix.GeneralMatrix;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.opengis.referencing.operation.MathTransform;

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
