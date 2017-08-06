//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package org.n52.tsf.model.raster.gt;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.matrix.GeneralMatrix;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.n52.tsf.model.raster.gt.beans.GeoPoint;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class provides the Geotools coverage data to ProtoBuf Deserialization functionality.
 */
public class AvroCovDataDeserializationHandler {

    public GridCoverage2D deserialize(String covName, InputStream inputStream) throws Exception {
        DatumReader<org.n52.tsf.serialization.avro.gen.raster.Grid> datumReader =
                new SpecificDatumReader<>(org.n52.tsf.serialization.avro.gen.raster.Grid.class);
        DataFileStream<org.n52.tsf.serialization.avro.gen.raster.Grid> dataFileReader = new DataFileStream<>(inputStream, datumReader);
        org.n52.tsf.serialization.avro.gen.raster.Grid avroGrid = null;
        if (dataFileReader.hasNext()) {
            avroGrid = dataFileReader.next();
        }
        GridCoverageFactory factory = new GridCoverageFactory();
        BufferedImage image = new BufferedImage(avroGrid.getMaxWidth(), avroGrid.getMaxHight(), avroGrid.getColorSpace());
        ReferencedEnvelope rEnvelope = new ReferencedEnvelope(0, avroGrid.getMaxWidth(), 0, avroGrid.getMaxHight(), null);
        MathTransform mathTransform;
        //TODO add model transformation support
        if (avroGrid.getXulc().isNaN()) {
            mathTransform = createMathtransformFromMetaData(avroGrid.getTiePoints().get(0), avroGrid.getPixelScale());
        } else {
            mathTransform = createMathtransformFromTfw(avroGrid);
        }
        CoordinateReferenceSystem crs = CRS.decode(avroGrid.getSourceCrs().toString());
        GeneralEnvelope envelope = CRS.transform(mathTransform, rEnvelope);
        envelope.setCoordinateReferenceSystem(crs);

        return factory.create(covName, image, envelope);
    }

    public List<GeoPoint> deserializeToWorld(InputStream inputStream) throws IOException {
        DatumReader<org.n52.tsf.serialization.avro.gen.raster.Grid> datumReader =
                new SpecificDatumReader<>(org.n52.tsf.serialization.avro.gen.raster.Grid.class);
        DataFileStream<org.n52.tsf.serialization.avro.gen.raster.Grid> dataFileReader = new DataFileStream<>(inputStream, datumReader);
        org.n52.tsf.serialization.avro.gen.raster.Grid avroGrid = null;
        if (dataFileReader.hasNext()) {
            avroGrid = dataFileReader.next();
        }

        if (avroGrid.getGridPoints().size() > 0) {
            return avroGrid.getGridPoints().
                    stream().map(this::createGeoPoint).collect(Collectors.toList());
        } else {
            return null;
        }
    }

    private MathTransform createMathtransformFromTfw(org.n52.tsf.serialization.avro.gen.raster.Grid pbGrid) {
        GeneralMatrix gm = new GeneralMatrix(3);
        gm.setElement(0, 0, pbGrid.getXPixelSize());
        gm.setElement(1, 1, pbGrid.getYPixelSize());
        gm.setElement(0, 1, pbGrid.getXRotation());
        gm.setElement(1, 0, pbGrid.getYRotation());
        gm.setElement(0, 2, pbGrid.getXulc());
        gm.setElement(1, 2, pbGrid.getYulc());
        return ProjectiveTransform.create(gm);
    }

    private static MathTransform createMathtransformFromMetaData(org.n52.tsf.serialization.avro.gen.raster.TiePoint tiePoint,
                                                                 org.n52.tsf.serialization.avro.gen.raster.PixelScale pixScales) {
        GeneralMatrix gm = new GeneralMatrix(3);
        gm.setElement(0, 0, pixScales.getScaleX());
        gm.setElement(1, 1, -pixScales.getScaleY());
        gm.setElement(0, 1, 0);
        gm.setElement(1, 0, 0);
        gm.setElement(0, 2, tiePoint.getValues().get(3));
        gm.setElement(1, 2, tiePoint.getValues().get(4));
        return ProjectiveTransform.create(gm);
    }

    private GeoPoint createGeoPoint(org.n52.tsf.serialization.avro.gen.raster.GridPoint gridPoint) {
        return new GeoPoint(gridPoint.getLongitude(), gridPoint.getLatitude());
    }
}
