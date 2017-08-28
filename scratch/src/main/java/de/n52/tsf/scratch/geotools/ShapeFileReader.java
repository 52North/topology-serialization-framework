package de.n52.tsf.scratch.geotools;

import java.io.File;
import java.util.ArrayList;


import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.shapefile.shp.ShapefileReader;

public class ShapeFileReader {

    public static void main(String[] args) throws Exception {
//        ClassLoader classLoader = ShapeFileReader.class.getClassLoader();
//        File file = new File(classLoader.getResource("shape/points.shp").getFile());
//        Map<String, Object> map = new HashMap<>();
//        map.put("url", file.toURI().toURL());
//
//        DataStore dataStore = DataStoreFinder.getDataStore(map);
//        String typeName = dataStore.getTypeNames()[0];
//
//        FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore
//                .getFeatureSource(typeName);
//        Filter filter = Filter.INCLUDE; // ECQL.toFilter("BBOX(THE_GEOM, 10,20,30,40)")
//
//        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures(filter);
//        try (FeatureIterator<SimpleFeature> features = collection.features()) {
//            while (features.hasNext()) {
//                SimpleFeature feature = features.next();
//                //System.out.println(feature.getID() + ": ");
//                System.out.println(feature.getDefaultGeometryProperty().getValue());
//
//            }
//        }
        testShapefileReaderRecord();
    }

    public static void testShapefileReaderRecord() throws Exception {
        ClassLoader classLoader = ShapeFileReader.class.getClassLoader();
        //Shape file reference - https://github.com/mbostock/shapefile/tree/master/test
        File file = new File(classLoader.getResource("shape/points.shp").getFile());
        ShpFiles shpFiles = new ShpFiles(file.toURI().toURL());
        ShapefileReader reader = new ShapefileReader(shpFiles, false, false, new GeometryFactory());
        ArrayList offsets = new ArrayList();

        while (reader.hasNext()) {
            ShapefileReader.Record record = reader.nextRecord();
            offsets.add(new Integer(record.offset()));

            Geometry geom = (Geometry) record.shape();
            System.out.println(geom.toString());
        }
        reader.close();

    }
}
