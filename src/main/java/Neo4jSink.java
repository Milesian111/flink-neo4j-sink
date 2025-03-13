/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.apache.flink.api.connector.sink2.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class Neo4jSink implements Sink<CypherStatement> {
    private final String uri;
    private final String user;
    private final String password;
    private final int batchSize;

    public Neo4jSink(String uri, String user, String password, int batchSize) {
        this.uri = uri;
        this.user = user;
        this.password = password;
        this.batchSize = batchSize;
    }

    @Override
    public SinkWriter<CypherStatement> createWriter(InitContext initContext) throws IOException {
        return new Neo4jSinkWriter(uri, user, password, batchSize);
    }

}
