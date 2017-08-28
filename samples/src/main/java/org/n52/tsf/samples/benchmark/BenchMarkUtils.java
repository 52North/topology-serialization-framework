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

package org.n52.tsf.samples.benchmark;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.shapefile.shp.ShapefileReader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BenchMarkUtils {
    public static final String FILE_OUTPUT = "output";
    public static final String FILE_INPUT = "input";

    public static GeometryCollection getGeometryCollection(File shapeFile) throws IOException {
        ShpFiles shpFiles = new ShpFiles(shapeFile.toURI().toURL());
        ShapefileReader reader = new ShapefileReader(shpFiles, false, false, new GeometryFactory());
        List<Geometry> geometries = new ArrayList<>();
        while (reader.hasNext()) {
            ShapefileReader.Record record = reader.nextRecord();
            geometries.add((Geometry) record.shape());
        }
        reader.close();
        return new GeometryFactory().createGeometryCollection(geometries.toArray(new Geometry[geometries.size()]));
    }

    public static void setupOptions(Options options) {
        Option input = Option.builder("i")
                .required(true)
                .argName(FILE_INPUT)
                .desc("Shape file input")
                .longOpt("input")
                .hasArg(true)
                .numberOfArgs(1)
                .type(String.class)
                .build();

        Option output = Option.builder("o")
                .required(false)
                .argName(FILE_OUTPUT)
                .desc("Serialize output file")
                .longOpt("output")
                .hasArg(true)
                .numberOfArgs(1)
                .type(String.class)
                .build();

        options.addOption(input);
        options.addOption(output);
    }
}
