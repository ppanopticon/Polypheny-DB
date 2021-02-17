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

package org.polypheny.db.ddl;


import java.util.Map;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.processing.QueryProcessor;
import org.polypheny.db.routing.Router;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.parser.SqlParserPos;

/**
 * Abstract class for the DDLManager, goal of this class is to expose a unified interface,
 * which allows to handle ddls. Especially with regard to different models.
 */
public abstract class DdlManager {

    public static DdlManager INSTANCE = null;


    public enum Language {
        SQL( 1 ),
        MQL( 2 );

        private final int id;


        Language( int id ) {
            this.id = id;
        }
    }


    /**
     * Sets a new DdlManager and returns it.
     *
     * @param manager the DdlManager which is set
     * @return the set instance of the DdlManager, which was set
     */
    public static DdlManager setAndGetInstance( DdlManager manager ) {
        if ( INSTANCE != null ) {
            throw new RuntimeException( "Overwriting the DdlManger, when already set is not permitted." );
        }
        INSTANCE = manager;
        return INSTANCE;
    }


    /**
     * Access Pattern for DdlManager Singleton.
     *
     * @return The DdlManager
     */
    public static DdlManager getInstance() {
        if ( INSTANCE == null ) {
            throw new RuntimeException( "DdlManager was not set correctly on Polypheny-DB start-up" );
        }
        return INSTANCE;
    }


    /**
     * Creates a schema with the provided options.
     *
     * @param name name of the desired schema
     * @param databaseId id of the database, to which the schema belongs
     * @param type the schema type, RELATIONAL, DOCUMENT, etc.
     * @param userId the id of executing user
     * @param ifNotExists if the schema only needs to be created when it not already exists
     * @param replace if the schema should replace another
     * @param parserPosition the position of the schema in the initial ddl command
     */
    public abstract void createSchema( String name, long databaseId, SchemaType type, int userId, boolean ifNotExists, boolean replace, SqlParserPos parserPosition );

    /**
     * Adds a new adapter with the name and the config options given to a store
     *
     * @param storeName name of the store for the adapter
     * @param adapterName the name of the adapter itself
     * @param config all provided options
     */
    public abstract void addAdapter( String storeName, String adapterName, Map<String, String> config );

    /**
     * Drops an adapter
     *
     * @param name name of the adapter to drop
     * @param router
     * @param processor
     * @param position
     */
    public abstract void dropAdapter( String name, Router router, QueryProcessor processor, SqlParserPos position );


    /**
     * Change the owner of a specific schema
     *
     * @param databaseId the id of the database of the schema
     * @param schemaName the name of the desired schema
     * @param ownerName the name of the owner
     * @param schemaPos the position of the schema in the initial query
     * @param ownerPos the position of the owner in the initial query
     */
    public abstract void alterSchemaOwner( String schemaName, String ownerName, long databaseId, SqlParserPos schemaPos, SqlParserPos ownerPos );

    /**
     * Change the name of the specific schema
     *
     * @param newName the new name of the schema
     * @param oldName the old name of the schema
     * @param databaseId the id of the database the schema belongs to
     * @param newNamePos the position of the new name in the initial query
     * @param oldNamePos the position of the old name in the initial query
     */
    public abstract void alterSchemaRename( String newName, String oldName, long databaseId, SqlParserPos newNamePos, SqlParserPos oldNamePos );

    /**
     * Adds a column to an existing table
     *
     * @param catalogTable the catalogTable
     * @param columnPhysicalName the physical name of the new column
     * @param columnLogicalName the name of the new column
     * @param beforeColumn the column before the column which is inserted; can be null
     * @param afterColumn the column after the column, which is inserted; can be null
     * @param defaultValue the default value of the inserted column
     * @param columnLogicalPos the position of the column in the initial query
     */
    public abstract void alterTableAddColumn( CatalogTable catalogTable, String columnPhysicalName, String columnLogicalName, CatalogColumn beforeColumn, CatalogColumn afterColumn, SqlNode defaultValue, SqlParserPos columnLogicalPos );

}
