/*
 * Copyright 2019-2020 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.adapter.mongodb;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.bson.Document;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.impl.AbstractSchema;


/**
 * Schema mapped onto a directory of MONGO files. Each table in the schema is a MONGO file in that directory.
 */
public class MongoSchema extends AbstractSchema {

    final MongoDatabase mongoDb;


    /**
     * Creates a MongoDB schema.
     *
     * @param host            Mongo host, e.g. "localhost"
     * @param credentialsList Optional credentials (empty list for none)
     * @param options         Mongo connection options
     * @param database        Mongo database name, e.g. "foodmart"
     */
    //public MongoSchema( String host, String database, List<MongoCredential> credentialsList, MongoClientOptions options ) { // TODO DL: evaluate what options are needed in the end
    public MongoSchema(final String host, final int port, String database) {
        super();
        try {
            final MongoClient mongo = new MongoClient(host, port);
            this.mongoDb = mongo.getDatabase(database);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Allows tests to inject their instance of the database.
     *
     * @param mongoDb existing mongo database instance
     */
    @VisibleForTesting
    MongoSchema(MongoDatabase mongoDb) {
        super();
        this.mongoDb = Objects.requireNonNull(mongoDb, "mongoDb");
    }


    @Override
    protected Map<String, Table> getTableMap() {
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        for (String collectionName : mongoDb.listCollectionNames()) {
            builder.put(collectionName, new MongoTable(collectionName));
        }
        return builder.build();
    }
}

