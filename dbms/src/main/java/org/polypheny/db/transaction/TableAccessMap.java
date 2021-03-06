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

package org.polypheny.db.transaction;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelVisitor;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.schema.LogicalTable;


/**
 * <code>TableAccessMap</code> represents the tables accessed by a query plan, with READ/WRITE information.
 */
public class TableAccessMap {

    /**
     * Access mode.
     */
    public enum Mode {
        /**
         * Table is not accessed at all.
         */
        NO_ACCESS,

        /**
         * Table is accessed for read only.
         */
        READ_ACCESS,

        /**
         * Table is accessed for write only.
         */
        WRITE_ACCESS,

        /**
         * Table is accessed for both read and write.
         */
        READWRITE_ACCESS
    }


    private final Map<TableIdentifier, Mode> accessMap;


    /**
     * Constructs a permanently empty TableAccessMap.
     */
    public TableAccessMap() {
        accessMap = Collections.emptyMap();
    }


    /**
     * Constructs a TableAccessMap for all tables accessed by a RelNode and its descendants.
     *
     * @param rel the RelNode for which to build the map
     */
    public TableAccessMap( RelNode rel ) {
        // NOTE: This method must NOT retain a reference to the input rel, because we use it for cached statements, and we
        // don't want to retain any rel references after preparation completes.
        accessMap = new HashMap<>();
        RelOptUtil.go( new TableRelVisitor(), rel );
    }


    /**
     * Constructs a TableAccessMap for a single table
     *
     * @param tableIdentifier fully qualified name of the table, represented as a list
     * @param mode            access mode for the table
     */
    public TableAccessMap( TableIdentifier tableIdentifier, Mode mode ) {
        accessMap = new HashMap<>();
        accessMap.put( tableIdentifier, mode );
    }


    /**
     * @return set of qualified names for all tables accessed
     */
    public Set<TableIdentifier> getTablesAccessed() {
        return accessMap.keySet();
    }


    /**
     * Determines whether a table is accessed at all.
     *
     * @param tableIdentifier qualified name of the table of interest
     * @return true if table is accessed
     */
    public boolean isTableAccessed( TableIdentifier tableIdentifier ) {
        return accessMap.containsKey( tableIdentifier );
    }


    /**
     * Determines whether a table is accessed for read.
     *
     * @param tableIdentifier qualified name of the table of interest
     * @return true if table is accessed for read
     */
    public boolean isTableAccessedForRead( TableIdentifier tableIdentifier ) {
        Mode mode = getTableAccessMode( tableIdentifier );
        return (mode == Mode.READ_ACCESS) || (mode == Mode.READWRITE_ACCESS);
    }


    /**
     * Determines whether a table is accessed for write.
     *
     * @param tableIdentifier qualified name of the table of interest
     * @return true if table is accessed for write
     */
    public boolean isTableAccessedForWrite( TableIdentifier tableIdentifier ) {
        Mode mode = getTableAccessMode( tableIdentifier );
        return (mode == Mode.WRITE_ACCESS) || (mode == Mode.READWRITE_ACCESS);
    }


    /**
     * Determines the access mode of a table.
     *
     * @param tableIdentifier qualified name of the table of interest
     * @return access mode
     */
    public Mode getTableAccessMode( @NonNull TableIdentifier tableIdentifier ) {
        Mode mode = accessMap.get( tableIdentifier );
        if ( mode == null ) {
            return Mode.NO_ACCESS;
        }
        return mode;
    }


    /**
     * Constructs a qualified name for an optimizer table reference.
     *
     * @param table table of interest
     * @return qualified name
     */
    public TableIdentifier getQualifiedName( RelOptTable table ) {
        if ( !(table instanceof RelOptTableImpl) ) {
            throw new RuntimeException( "Unexpected table type: " + table.getClass() );
        }
        if ( !(((RelOptTableImpl) table).getTable() instanceof LogicalTable) ) {
            throw new RuntimeException( "Unexpected table type: " + ((RelOptTableImpl) table).getTable().getClass() );
        }
        return new TableIdentifier( ((LogicalTable) ((RelOptTableImpl) table).getTable()).getTableId() );
    }


    /**
     * Visitor that finds all tables in a tree.
     */
    private class TableRelVisitor extends RelVisitor {

        @Override
        public void visit( RelNode p, int ordinal, RelNode parent ) {
            super.visit( p, ordinal, parent );
            RelOptTable table = p.getTable();
            if ( table == null ) {
                return;
            }
            Mode newAccess;

            // FIXME: Don't rely on object type here; eventually someone is going to write a rule which transforms to
            //  something which doesn't inherit TableModify, and this will break. Need to make this explicit in the
            //  RelNode interface.
            if ( p instanceof TableModify ) {
                newAccess = Mode.WRITE_ACCESS;
            } else {
                newAccess = Mode.READ_ACCESS;
            }
            TableIdentifier key = getQualifiedName( table );
            Mode oldAccess = accessMap.get( key );
            if ( (oldAccess != null) && (oldAccess != newAccess) ) {
                newAccess = Mode.READWRITE_ACCESS;
            }
            accessMap.put( key, newAccess );
        }
    }


    @Data
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class TableIdentifier {

        long tableId;
    }
}
