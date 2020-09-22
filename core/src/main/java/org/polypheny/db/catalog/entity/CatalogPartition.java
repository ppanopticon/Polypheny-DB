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
 */

package org.polypheny.db.catalog.entity;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.polypheny.db.catalog.Catalog;

import java.io.Serializable;
import java.util.List;

@EqualsAndHashCode
public final class CatalogPartition implements CatalogEntity, Comparable<CatalogTable> {


    private static final long serialVersionUID = 2312903632511266177L;

    //possibly a hash code to uniquely identify a partition
    // for a schema_table_column_type_argument
    // e.g. mySchema_testTable_sales_RANGE_100
    public final long id;

    public final long tableId;
    public final long schemaId;
    public final long databaseId;
    public final int ownerId;
    public final String ownerName;
    public final List partitionQualifiers;
    public final boolean isUnbound;

    //Technically a compound between primary key and partition column (+range/list)
    public final long partitionKey;



    public CatalogPartition(final long id,
                            final long tableId,
                            final long schemaId,
                            final long databaseId,
                            final int ownerId,
                            @NonNull final String ownerName,
                            final long partitionKey,
                            final List partitionQualifiers,
                            final boolean isUnbound) {

        this.id = id;
        this.tableId = tableId;
        this.schemaId = schemaId;
        this.databaseId = databaseId;
        this.ownerId = ownerId;
        this.ownerName = ownerName;
        this.partitionKey = partitionKey;
        this.partitionQualifiers = partitionQualifiers;
        this.isUnbound = isUnbound;

        System.out.println("\t\t\t\t\taddPartition" + " - " + partitionQualifiers);
        System.out.println("HENNLO: CatalogPartition: Partition has been created: " + id);
    }


    @SneakyThrows
    public String getTableName() {
        return Catalog.getInstance().getTable( tableId ).name;
    }

    @SneakyThrows
    public String getDatabaseName() {
        return Catalog.getInstance().getDatabase( databaseId ).name;
    }

    @SneakyThrows
    public String getSchemaName() {
        return Catalog.getInstance().getSchema( schemaId ).name;
    }

    @Override
    public int compareTo(CatalogTable catalogTable) {
        //TODO: To be implemented
        return 0;
    }

    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }
}
