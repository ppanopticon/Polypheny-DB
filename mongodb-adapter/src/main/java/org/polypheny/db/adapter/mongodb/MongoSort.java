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


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelFieldCollation;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.Util;


/**
 * Implementation of {@link Sort} relational expression in MongoDB.
 */
public class MongoSort extends Sort implements MongoRel {

    public MongoSort( RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RelCollation collation, RexNode offset, RexNode fetch ) {
        super( cluster, traitSet, child, collation, offset, fetch );
        assert getConvention() == CONVENTION;
        assert getConvention() == child.getConvention();
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.05 );
    }


    @Override
    public Sort copy( RelTraitSet traitSet, RelNode input, RelCollation newCollation, RexNode offset, RexNode fetch ) {
        return new MongoSort( getCluster(), traitSet, input, collation, offset, fetch );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.visitChild( 0, getInput() );
        if ( !collation.getFieldCollations().isEmpty() ) {
            final List<String> keys = new ArrayList<>();
            final List<RelDataTypeField> fields = getRowType().getFieldList();
            for ( RelFieldCollation fieldCollation : collation.getFieldCollations() ) {
                final String name =
                        fields.get( fieldCollation.getFieldIndex() ).getName();
                keys.add( name + ": " + direction( fieldCollation ) );
                if ( false ) {
                    // TODO: NULLS FIRST and NULLS LAST
                    switch ( fieldCollation.nullDirection ) {
                        case FIRST:
                            break;
                        case LAST:
                            break;
                    }
                }
            }
            implementor.add( null, "{$sort: " + Util.toString( keys, "{", ", ", "}" ) + "}" );
        }
        if ( offset != null ) {
            implementor.add( null, "{$skip: " + ((RexLiteral) offset).getValue() + "}" );
        }
        if ( fetch != null ) {
            implementor.add( null, "{$limit: " + ((RexLiteral) fetch).getValue() + "}" );
        }
    }


    private int direction( RelFieldCollation fieldCollation ) {
        switch ( fieldCollation.getDirection() ) {
            case DESCENDING:
            case STRICTLY_DESCENDING:
                return -1;
            case ASCENDING:
            case STRICTLY_ASCENDING:
            default:
                return 1;
        }
    }
}

