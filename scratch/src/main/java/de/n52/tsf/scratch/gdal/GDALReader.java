package de.n52.tsf.scratch.gdal;

import de.n52.tsf.scratch.geotools.WordFileReaderSample;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;

import java.io.File;

public class GDALReader {

    public static void main(String[] args){
        ClassLoader classLoader = WordFileReaderSample.class.getClassLoader();
        //source geo.tiff - https://github.com/geotools/geotools/tree/master/modules/plugin/geotiff/src/test/resources/org/geotools/gce/geotiff/test-data
        File file = new File(classLoader.getResource("geotif/geo.tiff").getFile());
        gdal.AllRegister();
        Dataset dataset = gdal.Open(file.getAbsolutePath());
        dataset.getRasterXSize();
    }
}
