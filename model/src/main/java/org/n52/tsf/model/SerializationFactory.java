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
import org.n52.tsf.model.vector.jts.locationtech.AvroSerializationHandlerLT;
import org.n52.tsf.model.vector.jts.locationtech.PBSerializationHandlerLT;
import org.n52.tsf.model.vector.jts.vividsolutions.AvroSerializationHandlerVS;
import org.n52.tsf.model.vector.jts.vividsolutions.PBSerializationHandlerVS;

import java.io.IOException;
import java.io.OutputStream;

public class SerializationFactory {
    private final static Logger logger = Logger.getLogger(SerializationFactory.class);

    public static SerializationHandler createSerializer(OutputStream outputStream, SerializerType serializerType) throws IOException {
        SerializationHandler serializationHandler = null;

        switch (serializerType){
            case AVRO_SERIALIZER_VS:
                serializationHandler = new AvroSerializationHandlerVS(outputStream);
                break;
            case PROTOBUF_SERIALIZER_VS:
                serializationHandler = new PBSerializationHandlerVS(outputStream);
                break;
            case AVRO_SERIALIZER_LT:
                serializationHandler = new AvroSerializationHandlerLT(outputStream);
                break;
            case PROTOBUF_SERIALIZER_LT:
                serializationHandler = new PBSerializationHandlerLT(outputStream);
                break;
            default:
                logger.error("Given serialization type does not support");
        }

        return serializationHandler;
    }
}
