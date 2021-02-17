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

package org.polypheny.db;

import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.swing.plaf.nimbus.State;
import org.apache.commons.lang3.StringUtils;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DataSource.ExportedColumn;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DataStore.AvailableIndexMethod;
import org.polypheny.db.adapter.index.IndexManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.catalog.Catalog.ForeignKeyOption;
import org.polypheny.db.catalog.Catalog.IndexType;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.exceptions.ColumnAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.SchemaAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.AlterSourceException;
import org.polypheny.db.ddl.exception.DdlOnSourceException;
import org.polypheny.db.ddl.exception.IndexExistsException;
import org.polypheny.db.ddl.exception.MissingColumnPlacementException;
import org.polypheny.db.ddl.exception.NotNullAndDefaultValueException;
import org.polypheny.db.ddl.exception.PlacementAlreadyExistsException;
import org.polypheny.db.ddl.exception.UnknownIndexMethodException;
import org.polypheny.db.processing.DataMigrator;
import org.polypheny.db.processing.QueryProcessor;
import org.polypheny.db.routing.Router;
import org.polypheny.db.runtime.PolyphenyDbContextException;
import org.polypheny.db.sql.SqlDataTypeSpec;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.type.PolyType;


public class DdlManagerImpl extends DdlManager {

    private final Catalog catalog;


    public DdlManagerImpl( Catalog catalog ) {
        this.catalog = catalog;
    }


    @Override
    public void createSchema( String name, long databaseId, SchemaType type, int userId, boolean ifNotExists, boolean replace ) throws SchemaAlreadyExistsException {
        // Check if there is already a schema with this name
        if ( catalog.checkIfExistsSchema( databaseId, name ) ) {
            if ( ifNotExists ) {
                // It is ok that there is already a schema with this name because "IF NOT EXISTS" was specified
                return;
            } else if ( replace ) {
                throw new RuntimeException( "Replacing schema is not yet supported." );
            } else {
                throw new SchemaAlreadyExistsException();
            }
        } else {
            long id = catalog.addSchema(
                    name,
                    databaseId,
                    userId,
                    type );
        }
    }


    @Override
    public void addAdapter( String storeName, String adapterName, Map<String, String> config ) {

        Adapter adapter = AdapterManager.getInstance().addAdapter( adapterName, storeName, config );
        if ( adapter instanceof DataSource ) {
            Map<String, List<ExportedColumn>> exportedColumns;
            try {
                exportedColumns = ((DataSource) adapter).getExportedColumns();
            } catch ( Exception e ) {
                AdapterManager.getInstance().removeAdapter( adapter.getAdapterId() );
                throw new RuntimeException( "Could not deploy adapter", e );
            }
            // Create table, columns etc.
            for ( Map.Entry<String, List<ExportedColumn>> entry : exportedColumns.entrySet() ) {
                // Make sure the table name is unique
                String tableName = entry.getKey();
                if ( catalog.checkIfExistsTable( 1, tableName ) ) {
                    int i = 0;
                    while ( catalog.checkIfExistsTable( 1, tableName + i ) ) {
                        i++;
                    }
                    tableName += i;
                }

                long tableId = catalog.addTable( tableName, 1, 1, TableType.SOURCE, !((DataSource) adapter).isDataReadOnly(), null );
                List<Long> primaryKeyColIds = new ArrayList<>();
                int colPos = 1;
                for ( ExportedColumn exportedColumn : entry.getValue() ) {
                    long columnId = catalog.addColumn(
                            exportedColumn.name,
                            tableId,
                            colPos++,
                            exportedColumn.type,
                            exportedColumn.collectionsType,
                            exportedColumn.length,
                            exportedColumn.scale,
                            exportedColumn.dimension,
                            exportedColumn.cardinality,
                            exportedColumn.nullable,
                            Collation.CASE_INSENSITIVE );
                    catalog.addColumnPlacement(
                            adapter.getAdapterId(),
                            columnId,
                            PlacementType.STATIC,
                            exportedColumn.physicalSchemaName,
                            exportedColumn.physicalTableName,
                            exportedColumn.physicalColumnName );
                    catalog.updateColumnPlacementPhysicalPosition( adapter.getAdapterId(), columnId, exportedColumn.physicalPosition );
                    if ( exportedColumn.primary ) {
                        primaryKeyColIds.add( columnId );
                    }
                }
                try {
                    catalog.addPrimaryKey( tableId, primaryKeyColIds );
                } catch ( GenericCatalogException e ) {
                    throw new RuntimeException( "Exception while adding primary key" );
                }
            }
        }
    }


    @Override
    public void dropAdapter( String name, Router router, QueryProcessor processor ) throws UnknownAdapterException {
        if ( name.startsWith( "'" ) ) {
            name = name.substring( 1 );
        }
        if ( name.endsWith( "'" ) ) {
            name = StringUtils.chop( name );
        }

        CatalogAdapter catalogAdapter = Catalog.getInstance().getAdapter( name );
        if ( catalogAdapter.type == AdapterType.SOURCE ) {
            Set<Long> tablesToDrop = new HashSet<>();
            for ( CatalogColumnPlacement ccp : catalog.getColumnPlacementsOnAdapter( catalogAdapter.id ) ) {
                tablesToDrop.add( ccp.tableId );
            }
            for ( Long tableId : tablesToDrop ) {
                CatalogTable table = catalog.getTable( tableId );

                // Make sure that there is only one adapter
                if ( table.placementsByAdapter.keySet().size() != 1 ) {
                    throw new RuntimeException( "The data source contains tables with more than one placement. This should not happen!" );
                }

                // Make sure table is of type source
                if ( table.tableType != TableType.SOURCE ) {
                    throw new RuntimeException( "Trying to drop a table located on a data source which is not of table type SOURCE. This should not happen!" );
                }

                // Inform routing
                router.dropPlacements( catalog.getColumnPlacementsOnAdapter( catalogAdapter.id, table.id ) );
                // Delete column placement in catalog
                for ( Long columnId : table.columnIds ) {
                    if ( catalog.checkIfExistsColumnPlacement( catalogAdapter.id, columnId ) ) {
                        catalog.deleteColumnPlacement( catalogAdapter.id, columnId );
                    }
                }

                // Delete keys and constraints
                try {
                    // Remove primary key
                    catalog.deletePrimaryKey( table.id );
                } catch ( GenericCatalogException e ) {
                    throw new PolyphenyDbContextException( "Exception while dropping primary key.", e );
                }

                // Delete columns
                for ( Long columnId : table.columnIds ) {
                    catalog.deleteColumn( columnId );
                }

                // Delete the table
                catalog.deleteTable( table.id );
            }

            // Rest plan cache and implementation cache
            processor.resetCaches();
        }

        AdapterManager.getInstance().removeAdapter( catalogAdapter.id );

    }


    @Override
    public void alterSchemaOwner( String schemaName, String ownerName, long databaseId ) throws UnknownUserException, UnknownSchemaException {

        CatalogSchema catalogSchema = catalog.getSchema( databaseId, schemaName );
        CatalogUser catalogUser = catalog.getUser( ownerName );
        catalog.setSchemaOwner( catalogSchema.id, catalogUser.id );

    }


    @Override
    public void alterSchemaRename( String newName, String oldName, long databaseId ) throws SchemaAlreadyExistsException, UnknownSchemaException {

        if ( catalog.checkIfExistsSchema( databaseId, newName ) ) {
            throw new SchemaAlreadyExistsException();
        }
        CatalogSchema catalogSchema = catalog.getSchema( databaseId, oldName );
        catalog.renameSchema( catalogSchema.id, newName );

    }


    @Override
    public void alterSourceTableAddColumn( CatalogTable catalogTable, String columnPhysicalName, String columnLogicalName, CatalogColumn beforeColumn, CatalogColumn afterColumn, SqlNode defaultValue, SqlParserPos columnLogicalPos ) {

        if ( catalog.checkIfExistsColumn( catalogTable.id, columnLogicalName ) ) {
            throw SqlUtil.newContextException( columnLogicalPos, RESOURCE.columnExists( columnLogicalName ) );
        }

        // Make sure that the table is of table type SOURCE
        if ( catalogTable.tableType != TableType.SOURCE ) {
            throw new RuntimeException( "Table '" + catalogTable.name + "' is not of type SOURCE!" );
        }

        // Make sure there is only one adapter
        if ( catalog.getColumnPlacements( catalogTable.columnIds.get( 0 ) ).size() != 1 ) {
            throw new RuntimeException( "The table has an unexpected number of placements!" );
        }

        int adapterId = catalog.getColumnPlacements( catalogTable.columnIds.get( 0 ) ).get( 0 ).adapterId;
        DataSource dataSource = (DataSource) AdapterManager.getInstance().getAdapter( adapterId );

        String physicalTableName = catalog.getColumnPlacements( catalogTable.columnIds.get( 0 ) ).get( 0 ).physicalTableName;
        List<ExportedColumn> exportedColumns = dataSource.getExportedColumns().get( physicalTableName );

        // Check if physicalColumnName is valid
        ExportedColumn exportedColumn = null;
        for ( ExportedColumn ec : exportedColumns ) {
            if ( ec.physicalColumnName.equalsIgnoreCase( columnPhysicalName ) ) {
                exportedColumn = ec;
            }
        }
        if ( exportedColumn == null ) {
            throw new RuntimeException( "Invalid physical column name '" + columnPhysicalName + "'!" );
        }

        // Make sure this physical column has not already been added to this table
        for ( CatalogColumnPlacement ccp : catalog.getColumnPlacementsOnAdapter( adapterId, catalogTable.id ) ) {
            if ( ccp.physicalColumnName.equalsIgnoreCase( columnPhysicalName ) ) {
                throw new RuntimeException( "The physical column '" + columnPhysicalName + "' has already been added to this table!" );
            }
        }

        List<CatalogColumn> columns = catalog.getColumns( catalogTable.id );
        int position = columns.size() + 1;
        if ( beforeColumn != null || afterColumn != null ) {
            if ( beforeColumn != null ) {
                position = beforeColumn.position;
            } else {
                position = afterColumn.position + 1;
            }
            // Update position of the other columns
            for ( int i = columns.size(); i >= position; i-- ) {
                catalog.setColumnPosition( columns.get( i - 1 ).id, i + 1 );
            }
        }

        long columnId = catalog.addColumn(
                columnLogicalName,
                catalogTable.id,
                position,
                exportedColumn.type,
                exportedColumn.collectionsType,
                exportedColumn.length,
                exportedColumn.scale,
                exportedColumn.dimension,
                exportedColumn.cardinality,
                exportedColumn.nullable,
                Collation.CASE_INSENSITIVE
        );
        CatalogColumn addedColumn = catalog.getColumn( columnId );

        // Add default value
        if ( defaultValue != null ) {
            // TODO: String is only a temporal solution for default values
            String v = defaultValue.toString();
            if ( v.startsWith( "'" ) ) {
                v = v.substring( 1, v.length() - 1 );
            }
            catalog.setDefaultValue( addedColumn.id, PolyType.VARCHAR, v );

            // Update addedColumn variable
            addedColumn = catalog.getColumn( columnId );
        }

        // Add column placement
        catalog.addColumnPlacement(
                adapterId,
                addedColumn.id,
                PlacementType.STATIC,
                exportedColumn.physicalSchemaName,
                exportedColumn.physicalTableName,
                exportedColumn.physicalColumnName );

        // Set column position
        catalog.updateColumnPlacementPhysicalPosition( adapterId, columnId, exportedColumn.physicalPosition );
    }


    @Override
    public void alterTableAddColumn( String columnName, CatalogTable catalogTable, CatalogColumn beforeColumn, CatalogColumn afterColumn, SqlDataTypeSpec type, boolean nullable, SqlNode defaultValue, Statement statement ) throws NotNullAndDefaultValueException, ColumnAlreadyExistsException {

        // Check if the column either allows null values or has a default value defined.
        if ( defaultValue == null && !nullable ) {
            throw new NotNullAndDefaultValueException();
        }

        if ( catalog.checkIfExistsColumn( catalogTable.id, columnName ) ) {
            throw new ColumnAlreadyExistsException( columnName, catalogTable.name );
        }

        List<CatalogColumn> columns = catalog.getColumns( catalogTable.id );
        int position = columns.size() + 1;
        if ( beforeColumn != null || afterColumn != null ) {
            if ( beforeColumn != null ) {
                position = beforeColumn.position;
            } else {
                position = afterColumn.position + 1;
            }
            // Update position of the other columns
            for ( int i = columns.size(); i >= position; i-- ) {
                catalog.setColumnPosition( columns.get( i - 1 ).id, i + 1 );
            }
        }
        final PolyType collectionsType = type.getCollectionsTypeName() == null ?
                null : PolyType.get( type.getCollectionsTypeName().getSimple() );
        long columnId = catalog.addColumn(
                columnName,
                catalogTable.id,
                position,
                PolyType.get( type.getTypeName().getSimple() ),
                collectionsType,
                type.getPrecision() == -1 ? null : type.getPrecision(),
                type.getScale() == -1 ? null : type.getScale(),
                type.getDimension() == -1 ? null : type.getDimension(),
                type.getCardinality() == -1 ? null : type.getCardinality(),
                nullable,
                Collation.CASE_INSENSITIVE
        );
        CatalogColumn addedColumn = catalog.getColumn( columnId );

        // Add default value
        if ( defaultValue != null ) {
            // TODO: String is only a temporal solution for default values
            String v = defaultValue.toString();
            if ( v.startsWith( "'" ) ) {
                v = v.substring( 1, v.length() - 1 );
            }
            catalog.setDefaultValue( addedColumn.id, PolyType.VARCHAR, v );

            // Update addedColumn variable
            addedColumn = catalog.getColumn( columnId );
        }

        // Ask router on which stores this column shall be placed
        List<DataStore> stores = statement.getRouter().addColumn( catalogTable, statement );

        // Add column on underlying data stores and insert default value
        for ( DataStore store : stores ) {
            catalog.addColumnPlacement(
                    store.getAdapterId(),
                    addedColumn.id,
                    PlacementType.AUTOMATIC,
                    null, // Will be set later
                    null, // Will be set later
                    null ); // Will be set later
            AdapterManager.getInstance().getStore( store.getAdapterId() ).addColumn( statement.getPrepareContext(), catalogTable, addedColumn );
        }

        // Rest plan cache and implementation cache (not sure if required in this case)
        statement.getQueryProcessor().resetCaches();
    }


    @Override
    public void alterTableAddForeignKey( CatalogTable catalogTable, CatalogTable refTable, List<String> columnNames, List<String> refColumnNames, SqlParserPos columnListPos, String constraintName, ForeignKeyOption onUpdate, ForeignKeyOption onDelete ) throws UnknownColumnException, GenericCatalogException {

        List<Long> columnIds = new LinkedList<>();
        for ( String columnName : columnNames ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( catalogTable.id, columnName );
            columnIds.add( catalogColumn.id );
        }
        List<Long> referencesIds = new LinkedList<>();
        for ( String columnName : refColumnNames ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( refTable.id, columnName );
            referencesIds.add( catalogColumn.id );
        }
        catalog.addForeignKey( catalogTable.id, columnIds, refTable.id, referencesIds, constraintName, onUpdate, onDelete );

    }


    @Override
    public void alterTableAddIndex( CatalogTable catalogTable, String indexMethodName, List<String> columnNames, String indexName, boolean isUnique, DataStore storeInstance, Statement statement ) throws UnknownColumnException, UnknownIndexMethodException, GenericCatalogException, UnknownTableException, UnknownUserException, UnknownSchemaException, UnknownKeyException, UnknownDatabaseException, TransactionException, AlterSourceException, IndexExistsException, MissingColumnPlacementException {

        List<Long> columnIds = new LinkedList<>();
        for ( String columnName : columnNames ) {
            CatalogColumn catalogColumn = catalog.getColumn( catalogTable.id, columnName );
            columnIds.add( catalogColumn.id );
        }

        IndexType type = IndexType.MANUAL;

        // Make sure that this is a table of type TABLE (and not SOURCE)
        if ( catalogTable.tableType != TableType.TABLE ) {
            throw new AlterSourceException();
        }

        // Check if there is already an index with this name for this table
        if ( catalog.checkIfExistsIndex( catalogTable.id, indexName ) ) {
            throw new IndexExistsException();
        }

        if ( storeInstance == null ) { // Polystore Index
            String method;
            String methodDisplayName;
            if ( indexMethodName != null ) {
                AvailableIndexMethod aim = null;
                for ( AvailableIndexMethod availableIndexMethod : IndexManager.getAvailableIndexMethods() ) {
                    if ( availableIndexMethod.name.equals( indexMethodName ) ) {
                        aim = availableIndexMethod;
                    }
                }
                if ( aim == null ) {
                    throw new UnknownIndexMethodException();
                }
                method = aim.name;
                methodDisplayName = aim.displayName;
            } else {
                method = IndexManager.getDefaultIndexMethod().name;
                methodDisplayName = IndexManager.getDefaultIndexMethod().displayName;
            }

            long indexId = catalog.addIndex(
                    catalogTable.id,
                    columnIds,
                    isUnique,
                    method,
                    methodDisplayName,
                    0,
                    type,
                    indexName );

            IndexManager.getInstance().addIndex( Catalog.getInstance().getIndex( indexId ), statement );
        } else { // Store Index

            // Check if there if all required columns are present on this store
            for ( long columnId : columnIds ) {
                if ( !catalog.checkIfExistsColumnPlacement( storeInstance.getAdapterId(), columnId ) ) {
                    throw new MissingColumnPlacementException( catalog.getColumn( columnId ).name );
                }
            }

            String method;
            String methodDisplayName;
            if ( indexMethodName != null ) {
                AvailableIndexMethod aim = null;
                for ( AvailableIndexMethod availableIndexMethod : storeInstance.getAvailableIndexMethods() ) {
                    if ( availableIndexMethod.name.equals( indexMethodName ) ) {
                        aim = availableIndexMethod;
                    }
                }
                if ( aim == null ) {
                    throw new UnknownIndexMethodException();
                }
                method = aim.name;
                methodDisplayName = aim.displayName;
            } else {
                method = storeInstance.getDefaultIndexMethod().name;
                methodDisplayName = storeInstance.getDefaultIndexMethod().displayName;
            }

            long indexId = catalog.addIndex(
                    catalogTable.id,
                    columnIds,
                    isUnique,
                    method,
                    methodDisplayName,
                    storeInstance.getAdapterId(),
                    type,
                    indexName );

            storeInstance.addIndex( statement.getPrepareContext(), catalog.getIndex( indexId ) );
        }

    }


    @Override
    public void alterTableAddPlacement( CatalogTable catalogTable, List<Long> columnIds, DataStore dataStore, Statement statement ) throws PlacementAlreadyExistsException {

        List<CatalogColumn> addedColumns = new LinkedList<>();

        // Check whether this placement already exists
        for ( int storeId : catalogTable.placementsByAdapter.keySet() ) {
            if ( storeId == dataStore.getAdapterId() ) {
                throw new PlacementAlreadyExistsException();
            }
        }
        // Check whether the list is empty (this is a short hand for a full placement)
        if ( columnIds.size() == 0 ) {
            columnIds = ImmutableList.copyOf( catalogTable.columnIds );
        }
        // Create column placements
        for ( long cid : columnIds ) {
            catalog.addColumnPlacement(
                    dataStore.getAdapterId(),
                    cid,
                    PlacementType.MANUAL,
                    null,
                    null,
                    null );
            addedColumns.add( catalog.getColumn( cid ) );
        }
        //Check if placement includes primary key columns
        CatalogPrimaryKey primaryKey = catalog.getPrimaryKey( catalogTable.primaryKey );
        for ( long cid : primaryKey.columnIds ) {
            if ( !columnIds.contains( cid ) ) {
                catalog.addColumnPlacement(
                        dataStore.getAdapterId(),
                        cid,
                        PlacementType.AUTOMATIC,
                        null,
                        null,
                        null );
                addedColumns.add( catalog.getColumn( cid ) );
            }
        }
        // Create table on store
        dataStore.createTable( statement.getPrepareContext(), catalogTable );
        // Copy data to the newly added placements
        DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
        dataMigrator.copyData( statement.getTransaction(), catalog.getAdapter( dataStore.getAdapterId() ), addedColumns );
    }


    @Override
    public void alterTableAddPrimaryKey( CatalogTable catalogTable, List<String> columnNames, Statement statement ) throws DdlOnSourceException {
        // Make sure that this is a table of type TABLE (and not SOURCE)
        if ( catalogTable.tableType != TableType.TABLE ) {
            throw new DdlOnSourceException();
        }

        try {
            CatalogPrimaryKey oldPk = catalog.getPrimaryKey( catalogTable.primaryKey );

            List<Long> columnIds = new LinkedList<>();
            for ( String columnName : columnNames ) {
                CatalogColumn catalogColumn = catalog.getColumn( catalogTable.id, columnName );
                columnIds.add( catalogColumn.id );
            }
            catalog.addPrimaryKey( catalogTable.id, columnIds );

            // Add new column placements
            long pkColumnId = oldPk.columnIds.get( 0 ); // It is sufficient to check for one because all get replicated on all stores
            List<CatalogColumnPlacement> oldPkPlacements = catalog.getColumnPlacements( pkColumnId );
            for ( CatalogColumnPlacement ccp : oldPkPlacements ) {
                for ( long columnId : columnIds ) {
                    if ( !catalog.checkIfExistsColumnPlacement( ccp.adapterId, columnId ) ) {
                        catalog.addColumnPlacement(
                                ccp.adapterId,
                                columnId,
                                PlacementType.AUTOMATIC,
                                null, // Will be set later
                                null, // Will be set later
                                null ); // Will be set later
                        AdapterManager.getInstance().getStore( ccp.adapterId ).addColumn(
                                statement.getPrepareContext(),
                                catalog.getTable( ccp.tableId ),
                                catalog.getColumn( columnId ) );
                    }
                }
            }
        } catch ( GenericCatalogException | UnknownColumnException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void alterTableAddUniqueConstraint( CatalogTable catalogTable, List<String> columnNames, String constraintName ) throws DdlOnSourceException {
        // Make sure that this is a table of type TABLE (and not SOURCE)
        if ( catalogTable.tableType != TableType.TABLE ) {
            throw new DdlOnSourceException();
        }

        try {
            List<Long> columnIds = new LinkedList<>();
            for ( String columnName : columnNames ) {
                CatalogColumn catalogColumn = Catalog.getInstance().getColumn( catalogTable.id, columnName );
                columnIds.add( catalogColumn.id );
            }
            Catalog.getInstance().addUniqueConstraint( catalogTable.id, constraintName, columnIds );
        } catch ( GenericCatalogException | UnknownColumnException e ) {
            throw new RuntimeException( e );
        }
    }


}
