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

package org.n52.tsf.model.jts;

import org.apache.log4j.Logger;
import org.locationtech.jts.geom.*;
import org.n52.tsf.serialization.protobuf.gen.GeoProtobuf;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Collectors;

/**
 * This class provides the JTS to ProtoBuf Serialization functionality.
 */
public class PBDeserializationHandler {
    final static Logger logger = Logger.getLogger(PBDeserializationHandler.class);
    GeometryFactory geometryFactory;

    public PBDeserializationHandler() {
        this.geometryFactory = new GeometryFactory();
    }

    public Geometry deserialize(InputStream inputStream) throws IOException {
        GeoProtobuf.Geometry pbGeometry = GeoProtobuf.Geometry.parseFrom(inputStream);
        Geometry jtsGeometry= null;
        switch (pbGeometry.getType()) {
            case POINT :
                jtsGeometry = deserializePoint(pbGeometry);
                break;
            case LINESTRING :
                jtsGeometry = deserializeLineString(pbGeometry);
                break;
            default:
                logger.error("Unsupported Geometric type");
        }
        return jtsGeometry;
    }

    private Point deserializePoint(GeoProtobuf.Geometry pbGeometry){
        Coordinate jtsCoordinate = createJtsCoordinate(pbGeometry.getCoordinates(0));
        Point point = geometryFactory.createPoint(jtsCoordinate);
        return point;
    }

    private LineString deserializeLineString(GeoProtobuf.Geometry pbGeometry){
        Coordinate[] jtsCoordinates = pbGeometry.getCoordinatesList().
                stream().map(this::createJtsCoordinate).collect(Collectors.toList()).stream().toArray(Coordinate[] :: new);
        LineString lineString = geometryFactory.createLineString(jtsCoordinates);
        return lineString;
    }

    private Coordinate createJtsCoordinate(GeoProtobuf.Coordinate pbCoordinate) {
        Coordinate jtsCoordinate = new Coordinate(pbCoordinate.getX(),pbCoordinate.getY());
        return jtsCoordinate;
    }
}
