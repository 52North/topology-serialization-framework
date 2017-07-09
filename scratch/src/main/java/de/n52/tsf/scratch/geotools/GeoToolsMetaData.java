package de.n52.tsf.scratch.geotools;

import it.geosolutions.imageio.plugins.tiff.BaselineTIFFTagSet;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.imageio.geotiff.GeoTiffIIOMetadataDecoder;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.io.File;
import java.io.IOException;

public class GeoToolsMetaData {

    public static void main (String[] args) throws IOException, TransformException, FactoryException {
        ClassLoader classLoader = GeoToolsTest.class.getClassLoader();

        //source no_crs_no_envelope2.tif - https://github.com/geotools/geotools/tree/master/modules/plugin/geotiff/src/test/resources/org/geotools/gce/geotiff/test-data
        File file = new File(classLoader.getResource("geotif/geo.tiff").getFile());
        extractMetaData(file);
    }

    public static void extractMetaData(File file) throws IOException, FactoryException {
        GeoTiffReader reader = new GeoTiffReader(file);
        GeoTiffIIOMetadataDecoder metadata = reader.getMetadata();

        GridCoverage2D coverage = reader.read(null);
        GridGeometry2D geometry = coverage.getGridGeometry();
        String readSoftware = metadata.getAsciiTIFFTag(Integer.toString(BaselineTIFFTagSet.TAG_SOFTWARE));
        String readCopyright = metadata.getAsciiTIFFTag(Integer.toString(BaselineTIFFTagSet.TAG_COPYRIGHT));
        System.out.println(BaselineTIFFTagSet.TAG_X_POSITION);
        System.out.println(BaselineTIFFTagSet.TAG_X_RESOLUTION);
        System.out.println(BaselineTIFFTagSet.TAG_COLOR_MAP);

        System.out.println(CRS.lookupIdentifier(coverage.getCoordinateReferenceSystem(), true));
        System.out.println(CRS.lookupEpsgCode(coverage.getCoordinateReferenceSystem(),true));
//
//        System.out.println(metadata.getModelTiePoints()[0]);
//        System.out.println(metadata.getModelPixelScales());

    }
}
