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

package org.n52.tsf.model.gt;

import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.n52.tsf.serialization.protobuf.gen.GeoProtobufCov;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.awt.image.BufferedImage;
import java.io.InputStream;

/**
 * This class provides the Geotools coverage data to ProtoBuf Deserialization functionality.
 */
public class PBCovDataDeserializationHandler {

    public GridCoverage2D deserialize(String covName, InputStream inputStream) throws Exception {
        GeoProtobufCov.Grid pbGrid = GeoProtobufCov.Grid.parseFrom(inputStream);
        GridCoverageFactory factory = new GridCoverageFactory();
        BufferedImage image = new BufferedImage(pbGrid.getMaxWidth(), pbGrid.getMaxHight(), pbGrid.getColorSpace());
        CoordinateReferenceSystem crs= CRS.decode(pbGrid.getSourceCrs());
        ReferencedEnvelope envelope = new ReferencedEnvelope(crs);
        return factory.create(covName, image, envelope);
    }
}
