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

package org.polypheny.db.sql.ddl.altertable;


import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlNodeList;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name ADD PLACEMENT [(columnList)] ON STORE storeName} statement.
 */
public class SqlAlterTableAddPlacement extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlNodeList columnList;
    private final SqlIdentifier storeName;
    List<Integer> partitionList;



    public SqlAlterTableAddPlacement( SqlParserPos pos, SqlIdentifier table, SqlNodeList columnList, SqlIdentifier storeName, List<Integer> partitionList ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.columnList = Objects.requireNonNull( columnList );
        this.storeName = Objects.requireNonNull( storeName );
        this.partitionList = partitionList;

        System.out.println("HENNNLO ->" + table + "->"+columnList +"->"+storeName +"->"+partitionList);
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( table, columnList, storeName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ADD" );
        writer.keyword( "PLACEMENT" );
        columnList.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ON" );
        writer.keyword( "STORE" );
        storeName.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement ) {
        CatalogTable catalogTable = getCatalogTable( context, table );
        Catalog catalog = Catalog.getInstance();
        //You can't partition placements if the table is not partitioned
        if (catalogTable.isPartitioned == false && !partitionList.isEmpty()){

            throw new RuntimeException(" Partition Placement is not allowed for unpartitioned table '"+ catalogTable.name +"'");
        }

        List<Long> columnIds = new LinkedList<>();
        for ( SqlNode node : columnList.getList() ) {
            CatalogColumn catalogColumn = getCatalogColumn( catalogTable.id, (SqlIdentifier) node );
            columnIds.add( catalogColumn.id );
        }
        Store storeInstance = StoreManager.getInstance().getStore( storeName.getSimple() );
        if ( storeInstance == null ) {
            throw SqlUtil.newContextException(
                    storeName.getParserPosition(),
                    RESOURCE.unknownStoreName( storeName.getSimple() ) );
        }
        try {
            // Check whether this placement already exists
            for ( int storeId : catalogTable.placementsByStore.keySet() ) {
                if ( storeId == storeInstance.getStoreId() ) {
                    throw SqlUtil.newContextException(
                            storeName.getParserPosition(),
                            RESOURCE.placementAlreadyExists( storeName.getSimple(), catalogTable.name ) );
                }
            }
            // Check whether the store supports schema changes
            if ( storeInstance.isSchemaReadOnly() ) {
                throw SqlUtil.newContextException(
                        storeName.getParserPosition(),
                        RESOURCE.storeIsSchemaReadOnly( storeName.getSimple() ) );
            }
            // Check whether the list is empty (this is a short hand for a full placement)
            if ( columnIds.size() == 0 ) {
                columnIds = ImmutableList.copyOf( catalogTable.columnIds );
            }


            List<Long> tempPartitionList = new ArrayList<Long>();
            //Select partitions to create on this placement
            if (catalogTable.isPartitioned) {
                boolean isDataPlacementPartitioned = false;

                //Needed to ensure that column placements on the same store contain all the same partitions
                //Check if this column placement is the first on the dataplacement
                //If this returns null this means that this is the first placement and partitition list can therefore be specified
                List<Long> currentPartList = new ArrayList<>();
                currentPartList = catalog.getPartitionsOnDataPlacement(storeInstance.getStoreId(), catalogTable.id);

                if (!currentPartList.isEmpty()) {
                    isDataPlacementPartitioned = true;
                }
                else {
                    isDataPlacementPartitioned = false;
                }

                if (!partitionList.isEmpty()) {

                    //Abort if a manual partitionList has been specified even though the data placemnt has already been partitioned
                    if ( isDataPlacementPartitioned ){
                        throw new RuntimeException("WARNING: The Data Placement for table: '" + catalogTable.name + "' on store: '"
                                + storeName + "' already contains manually specified partitions: " + currentPartList + ". Use 'ALTER TABLE ... MODIFY PARTITIONS...' instead");
                    }

                    System.out.println("HENNLO: SqlALterTableAddPlacement(): table is partitioned and concrete partitionList has been specified ");
                    //First convert specified index to correct partitionId
                    for (int partitionId: partitionList) {
                        //check if specified partition index is even part of table and if so get corresponding uniquePartId
                        try {
                            tempPartitionList.add(catalogTable.partitionIds.get(partitionId));
                        }catch (IndexOutOfBoundsException e){
                            throw new RuntimeException("Specified Partition-Index: '" + partitionId +"' is not part of table '"
                                    + catalogTable.name+"', has only " + catalogTable.numPartitions + " partitions");
                        }
                    }
                }
                //Simply Place all partitions on placement since nothing has been specified
                else if (partitionList.isEmpty()) {
                    System.out.println("HENNLO: SqlALterTableAddPlacement(): table is partitioned and concrete partitionList has NOT been specified ");

                    if ( isDataPlacementPartitioned ){
                        //If DataPlacement already contains partitions then create new placement with same set of partitions.
                        tempPartitionList = currentPartList;
                    }
                    else{
                        tempPartitionList = catalogTable.partitionIds;
                    }
                }
            }

            // Create column placements
            for ( long cid : columnIds ) {
                Catalog.getInstance().addColumnPlacement(
                        storeInstance.getStoreId(),
                        cid,
                        PlacementType.MANUAL,
                        null,
                        null,
                        null,
                        tempPartitionList);

            }
            //Check if placement includes primary key columns
            CatalogPrimaryKey primaryKey = Catalog.getInstance().getPrimaryKey( catalogTable.primaryKey );
            for ( long cid : primaryKey.columnIds ) {
                if ( !columnIds.contains( cid ) ) {
                    Catalog.getInstance().addColumnPlacement(
                            storeInstance.getStoreId(),
                            cid,
                            PlacementType.AUTOMATIC,
                            null,
                            null,
                            null,
                            tempPartitionList);
                }
            }

            // Create table on store
            storeInstance.createTable( context, catalogTable );



            // !!!!!!!!!!!!!!!!!!!!!!!!
            // TODO: Now we should also copy the data
        } catch ( GenericCatalogException | UnknownKeyException e ) {
            throw new RuntimeException( e );
        }
    }

}

