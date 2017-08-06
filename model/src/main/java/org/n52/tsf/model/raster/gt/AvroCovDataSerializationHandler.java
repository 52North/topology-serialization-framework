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

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.GridFormatFinder;
import org.geotools.coverage.grid.io.imageio.geotiff.GeoTiffIIOMetadataDecoder;
import org.geotools.coverage.grid.io.imageio.geotiff.TiePoint;
import org.geotools.data.WorldFileReader;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.geotools.referencing.CRS;
import org.n52.tsf.serialization.avro.gen.vector.Coordinate;
import org.n52.tsf.serialization.avro.gen.vector.Geometry;
import org.n52.tsf.serialization.protobuf.gen.GeoProtobufCov;
import org.opengis.coverage.grid.GridCoordinates;
import org.opengis.coverage.grid.GridEnvelope;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Double.NaN;

/**
 * This class provides the Geotools coverage data to ProtoBuf Serialization functionality.
 */
public class AvroCovDataSerializationHandler {

    public void serialize(File geotifFile, OutputStream outputStream, boolean serializeMetaData) throws Exception {
        org.n52.tsf.serialization.avro.gen.raster.Grid.Builder gridBuilder = org.n52.tsf.serialization.avro.gen.raster.Grid.newBuilder();
        if (serializeMetaData) {
            setMetaData(gridBuilder, geotifFile, null);
        } else {
            transformGridToWorld(gridBuilder, geotifFile);
        }
        DatumWriter<org.n52.tsf.serialization.avro.gen.raster.Grid> datumWriter =
                new SpecificDatumWriter<>(org.n52.tsf.serialization.avro.gen.raster.Grid.class);
        DataFileWriter<org.n52.tsf.serialization.avro.gen.raster.Grid> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(gridBuilder.build().getSchema(), outputStream);
        dataFileWriter.append(gridBuilder.build());
        dataFileWriter.close();
    }

    public void serialize(File geotifFile, File tfwfile, OutputStream outputStream) throws Exception {
        org.n52.tsf.serialization.avro.gen.raster.Grid.Builder gridBuilder = org.n52.tsf.serialization.avro.gen.raster.Grid.newBuilder();
        setMetaData(gridBuilder, geotifFile, tfwfile);
        DatumWriter<org.n52.tsf.serialization.avro.gen.raster.Grid> datumWriter =
                new SpecificDatumWriter<>(org.n52.tsf.serialization.avro.gen.raster.Grid.class);
        DataFileWriter<org.n52.tsf.serialization.avro.gen.raster.Grid> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(gridBuilder.build().getSchema(), outputStream);
        dataFileWriter.append(gridBuilder.build());
        dataFileWriter.close();
    }

    public void setMetaData(org.n52.tsf.serialization.avro.gen.raster.Grid.Builder gridBuilder, File geotifFile, File tfwFile) throws IOException, FactoryException {
        GeoTiffReader fileReader = new GeoTiffReader(geotifFile);
        GeoTiffIIOMetadataDecoder metadata = fileReader.getMetadata();
        AbstractGridFormat format = GridFormatFinder.findFormat(geotifFile);
        GridCoverage2DReader reader = format.getReader(geotifFile);
        GridEnvelope dimensions = reader.getOriginalGridRange();
        GridCoordinates maxDimensions = dimensions.getHigh();
        GridCoverage2D coverage = reader.read(null);
        gridBuilder.setMaxWidth(maxDimensions.getCoordinateValue(0) + 1);
        gridBuilder.setMaxHight(maxDimensions.getCoordinateValue(1) + 1);
        gridBuilder.setSourceCrs(CRS.lookupIdentifier(coverage.getCoordinateReferenceSystem(), true));
        gridBuilder.setColorSpace(coverage.getRenderedImage().getColorModel().getColorSpace().getType());

        List<org.n52.tsf.serialization.avro.gen.raster.GridPoint> gridPoints = new ArrayList<>();
        gridBuilder.setGridPoints(gridPoints);
        List<org.n52.tsf.serialization.avro.gen.raster.TiePoint> tiePoints = new ArrayList<>();
        if (metadata.hasTiePoints()) {
            for (TiePoint tp : metadata.getModelTiePoints()) {
                org.n52.tsf.serialization.avro.gen.raster.TiePoint.Builder tiePoint =
                        org.n52.tsf.serialization.avro.gen.raster.TiePoint.newBuilder();
                List<Double> values = new ArrayList<>();
                for (double value : tp.getData()) {
                    values.add(value);
                }
                tiePoint.setValues(values);
                tiePoints.add(tiePoint.build());
            }
        }
        gridBuilder.setTiePoints(tiePoints);

        org.n52.tsf.serialization.avro.gen.raster.PixelScale.Builder pixelScale =
                org.n52.tsf.serialization.avro.gen.raster.PixelScale.newBuilder();
        if (metadata.hasPixelScales()) {
            pixelScale.setScaleX(metadata.getModelPixelScales().getScaleX());
            pixelScale.setScaleY(metadata.getModelPixelScales().getScaleY());
            pixelScale.setScaleZ(metadata.getModelPixelScales().getScaleZ());
        } else {
            pixelScale.setScaleX(NaN);
            pixelScale.setScaleY(NaN);
            pixelScale.setScaleZ(NaN);
        }
        gridBuilder.setPixelScale(pixelScale.build());
        setTFWData(gridBuilder, tfwFile);
    }

    private void transformGridToWorld(org.n52.tsf.serialization.avro.gen.raster.Grid.Builder gridBuilder, File geotifFile) throws IOException, TransformException, FactoryException {
        AbstractGridFormat format = GridFormatFinder.findFormat(geotifFile);
        GridCoverage2DReader reader = format.getReader(geotifFile);
        GridEnvelope dimensions = reader.getOriginalGridRange();
        GridCoordinates maxDimensions = dimensions.getHigh();
        int w = maxDimensions.getCoordinateValue(0) + 1;
        int h = maxDimensions.getCoordinateValue(1) + 1;
        GridCoverage2D coverage = reader.read(null);
        GridGeometry2D geometry = coverage.getGridGeometry();
        List<org.n52.tsf.serialization.avro.gen.raster.GridPoint> gridPoints = new ArrayList<>();
        for (int i = 0; i < w; i++) {
            for (int j = 0; j < h; j++) {

                org.geotools.geometry.Envelope2D pixelEnvelop =
                        geometry.gridToWorld(new GridEnvelope2D(i, j, 1, 1));

                double latitude = pixelEnvelop.getCenterX();
                double longitude = pixelEnvelop.getCenterY();

                org.n52.tsf.serialization.avro.gen.raster.GridPoint.Builder gridPoint =
                        org.n52.tsf.serialization.avro.gen.raster.GridPoint.newBuilder();
                gridPoint.setLatitude(latitude);
                gridPoint.setLongitude(longitude);
                gridPoints.add(gridPoint.build());
            }
        }

        gridBuilder.setMaxWidth(w);
        gridBuilder.setMaxHight(h);
        gridBuilder.setSourceCrs(CRS.lookupIdentifier(coverage.getCoordinateReferenceSystem(), true));
        gridBuilder.setColorSpace(coverage.getRenderedImage().getColorModel().getColorSpace().getType());

        List<org.n52.tsf.serialization.avro.gen.raster.TiePoint> tiePoints = new ArrayList<>();
        gridBuilder.setTiePoints(tiePoints);
        org.n52.tsf.serialization.avro.gen.raster.PixelScale.Builder pixelScale =
                org.n52.tsf.serialization.avro.gen.raster.PixelScale.newBuilder();
        pixelScale.setScaleX(NaN);
        pixelScale.setScaleY(NaN);
        pixelScale.setScaleZ(NaN);
        gridBuilder.setPixelScale(pixelScale.build());
        gridBuilder.setGridPoints(gridPoints);
        setTFWData(gridBuilder, null);
    }

    private void setTFWData(org.n52.tsf.serialization.avro.gen.raster.Grid.Builder avroBuilder, File tfwFile) throws IOException {
        if (tfwFile != null) {
            WorldFileReader worldFileReader = new WorldFileReader(tfwFile);
            avroBuilder.setXulc(worldFileReader.getXULC());
            avroBuilder.setYulc(worldFileReader.getYULC());
            avroBuilder.setXPixelSize(worldFileReader.getXPixelSize());
            avroBuilder.setYPixelSize(worldFileReader.getYPixelSize());
            avroBuilder.setXRotation(worldFileReader.getRotationX());
            avroBuilder.setYRotation(worldFileReader.getRotationY());
        } else {
            avroBuilder.setXulc(NaN);
            avroBuilder.setYulc(NaN);
            avroBuilder.setXPixelSize(NaN);
            avroBuilder.setYPixelSize(NaN);
            avroBuilder.setXRotation(NaN);
            avroBuilder.setYRotation(NaN);
        }
    }
}
