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

import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DataSource.ExportedColumn;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.processing.QueryProcessor;
import org.polypheny.db.routing.Router;
import org.polypheny.db.runtime.PolyphenyDbContextException;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.type.PolyType;


public class DdlManagerImpl extends DdlManager {

    private final Catalog catalog;


    public DdlManagerImpl( Catalog catalog ) {
        this.catalog = catalog;
    }


    @Override
    public void createSchema( String name, long databaseId, SchemaType type, int userId, boolean ifNotExists, boolean replace, SqlParserPos parserPosition ) {
        // Check if there is already a schema with this name
        if ( catalog.checkIfExistsSchema( databaseId, name ) ) {
            if ( ifNotExists ) {
                // It is ok that there is already a schema with this name because "IF NOT EXISTS" was specified
                return;
            } else if ( replace ) {
                throw new RuntimeException( "Replacing schema is not yet supported." );
            } else {
                throw SqlUtil.newContextException( parserPosition, RESOURCE.schemaExists( name ) );
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
    public void dropAdapter( String name, Router router, QueryProcessor processor, SqlParserPos position ) {
        if ( name.startsWith( "'" ) ) {
            name = name.substring( 1 );
        }
        if ( name.endsWith( "'" ) ) {
            name = StringUtils.chop( name );
        }

        try {
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
        } catch ( UnknownAdapterException e ) {
            throw SqlUtil.newContextException( position, RESOURCE.unknownAdapter( e.getAdapterName() ) );
        } catch ( Exception e ) {
            throw new RuntimeException( "Could not remove data source from the adapter with the unique name '" + name + "'!", e );
        }
    }


    @Override
    public void alterSchemaOwner( String schemaName, String ownerName, long databaseId, SqlParserPos schemaPos, SqlParserPos ownerPos ) {
        try {
            CatalogSchema catalogSchema = catalog.getSchema( databaseId, schemaName );
            CatalogUser catalogUser = catalog.getUser( ownerName );
            catalog.setSchemaOwner( catalogSchema.id, catalogUser.id );
        } catch ( UnknownSchemaException e ) {
            throw SqlUtil.newContextException( schemaPos, RESOURCE.schemaNotFound( schemaName ) );
        } catch ( UnknownUserException e ) {
            throw SqlUtil.newContextException( ownerPos, RESOURCE.userNotFound( ownerName ) );
        }
    }


    @Override
    public void alterSchemaRename( String newName, String oldName, long databaseId, SqlParserPos newNamePos, SqlParserPos oldNamePos ) {
        try {
            Catalog catalog = Catalog.getInstance();
            if ( catalog.checkIfExistsSchema( databaseId, newName ) ) {
                throw SqlUtil.newContextException( oldNamePos, RESOURCE.schemaExists( newName ) );
            }
            CatalogSchema catalogSchema = catalog.getSchema( databaseId, oldName );
            catalog.renameSchema( catalogSchema.id, newName );
        } catch ( UnknownSchemaException e ) {
            throw SqlUtil.newContextException( oldNamePos, RESOURCE.schemaNotFound( oldName ) );
        }
    }


    @Override
    public void alterTableAddColumn( CatalogTable catalogTable, String columnPhysicalName, String columnLogicalName, CatalogColumn beforeColumn, CatalogColumn afterColumn, SqlNode defaultValue, SqlParserPos columnLogicalPos ) {

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
        CatalogAdapter catalogAdapter = catalog.getAdapter( adapterId );
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

}
