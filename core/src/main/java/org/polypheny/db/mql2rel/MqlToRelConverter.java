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

package org.polypheny.db.mql2rel;


import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.mql.MqlNode;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable.ViewExpander;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelCollations;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.metadata.JaninoRelMetadataProvider;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlDynamicParam;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlOperatorTable;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.sql.validate.SqlValidator;
import org.polypheny.db.sql2rel.SqlRexConvertletTable;
import org.polypheny.db.sql2rel.SubQueryConverter;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * Converts a SQL parse tree (consisting of {@link SqlNode} objects) into a relational algebra expression (consisting of {@link RelNode} objects).
 */
public class MqlToRelConverter {

    protected static final Logger SQL2REL_LOGGER = PolyphenyDbTrace.getSqlToRelTracer();

    private static final BigDecimal TWO = BigDecimal.valueOf( 2L );


    protected final SqlValidator validator;
    protected final RexBuilder rexBuilder;
    protected final CatalogReader catalogReader;
    protected final RelOptCluster cluster;
    private SubQueryConverter subQueryConverter;
    protected final List<RelNode> leaves = new ArrayList<>();
    private final List<SqlDynamicParam> dynamicParamSqlNodes = new ArrayList<>();
    private final SqlOperatorTable opTab;
    protected final RelDataTypeFactory typeFactory;
    private final MqlNodeToRexConverter exprConverter;
    private int explainParamCount;


    /**
     * Stack of names of datasets requested by the <code>TABLE(SAMPLE(&lt;datasetName&gt;, &lt;query&gt;))</code> construct.
     */
    private final Deque<String> datasetStack = new ArrayDeque<>();

    /**
     * Mapping of non-correlated sub-queries that have been converted to their equivalent constants. Used to avoid re-evaluating the sub-query if it's already been evaluated.
     */
    private final Map<SqlNode, RexNode> mapConvertedNonCorrSubqs = new HashMap<>();

    public final ViewExpander viewExpander;


    /* Creates a converter. */
    public MqlToRelConverter( ViewExpander viewExpander, SqlValidator validator, CatalogReader catalogReader, RelOptCluster cluster, SqlRexConvertletTable convertletTable, Config config ) {
        this.viewExpander = viewExpander;
        this.opTab =
                (validator == null)
                        ? SqlStdOperatorTable.instance()
                        : validator.getOperatorTable();
        this.validator = validator;
        this.catalogReader = catalogReader;
        /*this.subQueryConverter = new NoOpSubQueryConverter();*/
        this.rexBuilder = cluster.getRexBuilder();
        this.typeFactory = rexBuilder.getTypeFactory();
        this.cluster = Objects.requireNonNull( cluster );
        this.exprConverter = new MqlNodeToRexConverterImpl( convertletTable );
        this.explainParamCount = 0;

    }


    /**
     * Converts an unvalidated query's parse tree into a relational expression.
     *
     * @param query Query to convert
     * @param top Whether the query is top-level, say if its result will become a JDBC result set; <code>false</code> if the query will be part of a view.
     */
    public RelRoot convertQuery( MqlNode query, Statement statement, final boolean top ) {

        RelMetadataQuery.THREAD_PROVIDERS.set( JaninoRelMetadataProvider.of( cluster.getMetadataProvider() ) );
        //TableScanFactory factory = TableScanFactory;
        //RelNode result = TableScanFactory.createScan(  )
        /*if ( top ) {
            if ( isStream( query ) ) {
                result = new LogicalDelta( cluster, result.getTraitSet(), result );
            }
        }*/
        RelCollation collation = RelCollations.EMPTY;
        /*if ( !query.isA( SqlKind.DML ) ) {
            if ( isOrdered( query ) ) {
                collation = requiredCollation( result );
            }
        }*/
        //checkConvertedType( query, result );

        //final RelDataType validatedRowType = validator.getValidatedNodeType( null );

        Catalog catalog = Catalog.getInstance();

        RelNode node = statement.getRouter().buildJoinedTableScan( statement, cluster, catalog.getColumnPlacements( 0 ) );
        return RelRoot.of( node, query.getKind() );
    }


}

