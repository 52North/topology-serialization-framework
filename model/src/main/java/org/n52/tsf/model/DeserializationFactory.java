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

package org.n52.tsf.model;

import org.apache.log4j.Logger;
import org.n52.tsf.model.vector.jts.locationtech.AvroDeserializationHandlerLT;
import org.n52.tsf.model.vector.jts.locationtech.PBDeserializationHandlerLT;
import org.n52.tsf.model.vector.jts.vividsolutions.AvroDeserializationHandlerVS;
import org.n52.tsf.model.vector.jts.vividsolutions.PBDeserializationHandlerVS;

import java.io.IOException;
import java.io.InputStream;

public class DeserializationFactory {
    private final static Logger logger = Logger.getLogger(DeserializationFactory.class);

    public static DeserializationHandler createDeserializer(InputStream inputStream, DeserializerType deserializerType) throws IOException {
       DeserializationHandler deserializationHandler = null;

        switch (deserializerType){
            case AVRO_DESERIALIZER_VS:
                deserializationHandler = new AvroDeserializationHandlerVS(inputStream);
                break;
            case PROTOBUF_DESERIALIZER_VS:
                deserializationHandler= new PBDeserializationHandlerVS(inputStream);
                break;
            case AVRO_DESERIALIZER_LT:
                deserializationHandler = new AvroDeserializationHandlerLT(inputStream);
                break;
            case PROTOBUF_DESERIALIZER_LT:
                deserializationHandler = new PBDeserializationHandlerLT(inputStream);
                break;
            default:
                logger.error("Given deserialization type does not support");
        }

        return deserializationHandler;
    }
}
