/*
 * Copyright 2019-2021 The Polypheny Project
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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.processing.DataMigrator;
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
 * Parse tree for {@code ALTER TABLE name MODIFY PLACEMENT (columnList) ON STORE storeName} statement.
 */
@Slf4j
public class SqlAlterTableModifyPlacement extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlNodeList columnList;
    private final SqlIdentifier storeName;
    private final List<Integer> partitionList;
    private final List<SqlIdentifier> partitionNamesList;


    public SqlAlterTableModifyPlacement(
            SqlParserPos pos,
            SqlIdentifier table,
            SqlNodeList columnList,
            SqlIdentifier storeName,
            List<Integer> partitionList,
            List<SqlIdentifier> partitionNamesList ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.columnList = Objects.requireNonNull( columnList );
        this.storeName = Objects.requireNonNull( storeName );
        this.partitionList = partitionList;
        this.partitionNamesList = partitionNamesList;
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( table, columnList, storeName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        // TODO @HENNLO: This seems to be incomplete
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "MODIFY" );
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

        // You can't partition placements if the table is not partitioned
        if ( !catalogTable.isPartitioned && (!partitionList.isEmpty() || !partitionNamesList.isEmpty()) ) {
            throw new RuntimeException( " Partition Placement is not allowed for unpartitioned table '" + catalogTable.name + "'" );
        }

        List<Long> columnIds = new LinkedList<>();
        for ( SqlNode node : columnList.getList() ) {
            CatalogColumn catalogColumn = getCatalogColumn( catalogTable.id, (SqlIdentifier) node );
            columnIds.add( catalogColumn.id );
        }
        DataStore storeInstance = getDataStoreInstance( storeName );
        // Check whether this placement already exists
        if ( !catalogTable.placementsByAdapter.containsKey( storeInstance.getAdapterId() ) ) {
            throw SqlUtil.newContextException(
                    storeName.getParserPosition(),
                    RESOURCE.placementDoesNotExist( storeName.getSimple(), catalogTable.name ) );
        }

        // Which columns to remove
        for ( CatalogColumnPlacement placement : Catalog.getInstance().getColumnPlacementsOnAdapter( storeInstance.getAdapterId(), catalogTable.id ) ) {
            if ( !columnIds.contains( placement.columnId ) ) {
                // Check whether there are any indexes located on the store requiring this column
                for ( CatalogIndex index : Catalog.getInstance().getIndexes( catalogTable.id, false ) ) {
                    if ( index.location == storeInstance.getAdapterId() && index.key.columnIds.contains( placement.columnId ) ) {
                        throw SqlUtil.newContextException(
                                storeName.getParserPosition(),
                                RESOURCE.indexPreventsRemovalOfPlacement( index.name, Catalog.getInstance().getColumn( placement.columnId ).name ) );
                    }
                }
                // Check whether the column is a primary key column
                CatalogPrimaryKey primaryKey = Catalog.getInstance().getPrimaryKey( catalogTable.primaryKey );
                if ( primaryKey.columnIds.contains( placement.columnId ) ) {
                    // Check if the placement type is manual. If so, change to automatic
                    if ( placement.placementType == PlacementType.MANUAL ) {
                        // Make placement manual
                        Catalog.getInstance().updateColumnPlacementType(
                                storeInstance.getAdapterId(),
                                placement.columnId,
                                PlacementType.AUTOMATIC );
                    }
                } else {
                    // It is not a primary key. Remove the column
                    // Check if there are is another placement for this column
                    List<CatalogColumnPlacement> existingPlacements = Catalog.getInstance().getColumnPlacements( placement.columnId );
                    if ( existingPlacements.size() < 2 ) {
                        throw SqlUtil.newContextException( storeName.getParserPosition(), RESOURCE.onlyOnePlacementLeft() );
                    }
                    // Check if this placement would be the last columnPlacement with all partitions
                    if ( catalogTable.isPartitioned ) {
                        PartitionManagerFactory managerFactory = new PartitionManagerFactory();
                        PartitionManager partitionManager = managerFactory.getInstance( catalogTable.partitionType );

                        if ( !partitionManager.probePartitionDistributionChange( catalogTable, placement.adapterId, placement.columnId ) ) {
                            throw new RuntimeException( "Validation of partition distribution failed. Placement: '"
                                    + placement.adapterUniqueName + "." + placement.getLogicalColumnName() + "' would be the last ColumnPlacement with all partitions!" );
                        }
                    }
                    // Drop Column on store
                    storeInstance.dropColumn( context, Catalog.getInstance().getColumnPlacement( storeInstance.getAdapterId(), placement.columnId ) );
                    // Drop column placement
                    Catalog.getInstance().deleteColumnPlacement( storeInstance.getAdapterId(), placement.columnId );
                }
            }
        }

        List<Long> tempPartitionList = new ArrayList<>();
        // Select partitions to create on this placement
        if ( catalogTable.isPartitioned ) {
            long tableId = catalogTable.id;
            // If index partitions are specified
            if ( !partitionList.isEmpty() && partitionNamesList.isEmpty() ) {
                // First convert specified index to correct partitionId
                for ( int partitionId : partitionList ) {
                    // Check if specified partition index is even part of table and if so get corresponding uniquePartId
                    try {
                        tempPartitionList.add( catalogTable.partitionIds.get( partitionId ) );
                    } catch ( IndexOutOfBoundsException e ) {
                        throw new RuntimeException( "Specified Partition-Index: '" + partitionId + "' is not part of table '"
                                + catalogTable.name + "', has only " + catalogTable.numPartitions + " partitions" );
                    }
                }
                catalog.updatePartitionsOnDataPlacement( storeInstance.getAdapterId(), catalogTable.id, tempPartitionList );
            }
            // If name partitions are specified
            else if ( !partitionNamesList.isEmpty() && partitionList.isEmpty() ) {
                List<CatalogPartition> catalogPartitions = catalog.getPartitions( tableId );
                for ( String partitionName : partitionNamesList.stream().map( Object::toString ).collect( Collectors.toList() ) ) {
                    boolean isPartOfTable = false;
                    for ( CatalogPartition catalogPartition : catalogPartitions ) {
                        if ( partitionName.equals( catalogPartition.partitionName.toLowerCase() ) ) {
                            tempPartitionList.add( catalogPartition.id );
                            isPartOfTable = true;
                            break;
                        }
                    }
                    if ( !isPartOfTable ) {
                        throw new RuntimeException( "Specified partition name: '" + partitionName + "' is not part of table '"
                                + catalogTable.name + "'. Available partitions: " + String.join( ",", catalog.getPartitionNames( tableId ) ) );
                    }
                }
                catalog.updatePartitionsOnDataPlacement( storeInstance.getAdapterId(), catalogTable.id, tempPartitionList );
            }
        }

        // Which columns to add
        List<CatalogColumn> addedColumns = new LinkedList<>();
        for ( long cid : columnIds ) {
            if ( Catalog.getInstance().checkIfExistsColumnPlacement( storeInstance.getAdapterId(), cid ) ) {
                CatalogColumnPlacement placement = Catalog.getInstance().getColumnPlacement( storeInstance.getAdapterId(), cid );
                if ( placement.placementType == PlacementType.AUTOMATIC ) {
                    // Make placement manual
                    Catalog.getInstance().updateColumnPlacementType( storeInstance.getAdapterId(), cid, PlacementType.MANUAL );
                }
            } else {
                // Create column placement
                Catalog.getInstance().addColumnPlacement(
                        storeInstance.getAdapterId(),
                        cid,
                        PlacementType.MANUAL,
                        null,
                        null,
                        null,
                        tempPartitionList );
                // Add column on store
                storeInstance.addColumn( context, catalogTable, Catalog.getInstance().getColumn( cid ) );
                // Add to list of columns for which we need to copy data
                addedColumns.add( Catalog.getInstance().getColumn( cid ) );
            }
        }
        // Copy the data to the newly added column placements
        DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
        if ( addedColumns.size() > 0 ) {
            dataMigrator.copyData( statement.getTransaction(), Catalog.getInstance().getAdapter( storeInstance.getAdapterId() ), addedColumns );
        }

    }

}
