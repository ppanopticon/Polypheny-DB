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

package org.polypheny.db.prepare;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.FunctionExpression;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable.ToRelContext;
import org.polypheny.db.plan.RelOptTable.ViewExpander;
import org.polypheny.db.plan.ViewExpanders;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.BuiltInMethod;


/**
 * Translates a tree of linq4j {@link Queryable} nodes to a tree of {@link RelNode} planner nodes.
 *
 * @see QueryableRelBuilder
 */
class LixToRelTranslator {

    final RelOptCluster cluster;
    private final Prepare preparingStmt;
    final JavaTypeFactory typeFactory;


    LixToRelTranslator( RelOptCluster cluster, Prepare preparingStmt ) {
        this.cluster = cluster;
        this.preparingStmt = preparingStmt;
        this.typeFactory = (JavaTypeFactory) cluster.getTypeFactory();
    }


    ToRelContext toRelContext() {
        if ( preparingStmt instanceof ViewExpander ) {
            final ViewExpander viewExpander = (ViewExpander) this.preparingStmt;
            return ViewExpanders.toRelContext( viewExpander, cluster );
        } else {
            return ViewExpanders.simpleContext( cluster );
        }
    }


    public <T> RelNode translate( Queryable<T> queryable ) {
        QueryableRelBuilder<T> translatorQueryable = new QueryableRelBuilder<>( this );
        return translatorQueryable.toRel( queryable );
    }


    public RelNode translate( Expression expression ) {
        if ( expression instanceof MethodCallExpression ) {
            final MethodCallExpression call = (MethodCallExpression) expression;
            BuiltInMethod method = BuiltInMethod.MAP.get( call.method );
            if ( method == null ) {
                throw new UnsupportedOperationException( "unknown method " + call.method );
            }
            RelNode input;
            switch ( method ) {
                case SELECT:
                    input = translate( call.targetExpression );
                    return LogicalProject.create(
                            input,
                            toRex( input, (FunctionExpression) call.expressions.get( 0 ) ),
                            (List<String>) null );

                case WHERE:
                    input = translate( call.targetExpression );
                    return LogicalFilter.create(
                            input,
                            toRex( (FunctionExpression) call.expressions.get( 0 ), input ) );

                case AS_QUERYABLE:
                    return LogicalTableScan.create(
                            cluster,
                            RelOptTableImpl.create(
                                    null,
                                    typeFactory.createJavaType( Types.toClass( Types.getElementType( call.targetExpression.getType() ) ) ),
                                    ImmutableList.of(),
                                    call.targetExpression ) );

                case SCHEMA_GET_TABLE:
                    return LogicalTableScan.create(
                            cluster,
                            RelOptTableImpl.create(
                                    null,
                                    typeFactory.createJavaType( (Class) ((ConstantExpression) call.expressions.get( 1 )).value ),
                                    ImmutableList.of(),
                                    call.targetExpression ) );

                default:
                    throw new UnsupportedOperationException( "unknown method " + call.method );
            }
        }
        throw new UnsupportedOperationException( "unknown expression type " + expression.getNodeType() );
    }


    private List<RexNode> toRex( RelNode child, FunctionExpression expression ) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        List<RexNode> list = Collections.singletonList( rexBuilder.makeRangeReference( child ) );
        PolyphenyDbPrepareImpl.ScalarTranslator translator =
                PolyphenyDbPrepareImpl.EmptyScalarTranslator
                        .empty( rexBuilder )
                        .bind( expression.parameterList, list );
        final List<RexNode> rexList = new ArrayList<>();
        final Expression simple = Blocks.simple( expression.body );
        for ( Expression expression1 : fieldExpressions( simple ) ) {
            rexList.add( translator.toRex( expression1 ) );
        }
        return rexList;
    }


    List<Expression> fieldExpressions( Expression expression ) {
        if ( expression instanceof NewExpression ) {
            // Note: We are assuming that the arguments to the constructor are the same order as the fields of the class.
            return ((NewExpression) expression).arguments;
        }
        throw new RuntimeException( "unsupported expression type " + expression );
    }


    List<RexNode> toRexList( FunctionExpression expression, RelNode... inputs ) {
        List<RexNode> list = new ArrayList<>();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        for ( RelNode input : inputs ) {
            list.add( rexBuilder.makeRangeReference( input ) );
        }
        return PolyphenyDbPrepareImpl.EmptyScalarTranslator.empty( rexBuilder )
                .bind( expression.parameterList, list )
                .toRexList( expression.body );
    }


    RexNode toRex( FunctionExpression expression, RelNode... inputs ) {
        List<RexNode> list = new ArrayList<>();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        for ( RelNode input : inputs ) {
            list.add( rexBuilder.makeRangeReference( input ) );
        }
        return PolyphenyDbPrepareImpl.EmptyScalarTranslator.empty( rexBuilder )
                .bind( expression.parameterList, list )
                .toRex( expression.body );
    }
}
