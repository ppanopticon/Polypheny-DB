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

package org.polypheny.db.mql;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.polypheny.db.jdbc.JavaTypeFactoryImpl;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelTrait;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.rel.RelCollationImpl;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelShuttle;
import org.polypheny.db.rel.RelVisitor;
import org.polypheny.db.rel.RelWriter;
import org.polypheny.db.rel.SingleRel;
import org.polypheny.db.rel.core.CorrelationId;
import org.polypheny.db.rel.metadata.Metadata;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.rel.type.RelRecordType;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.type.BasicPolyType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Litmus;

public class TableChange implements RelNode {


    @Override
    public int getId() {
        return 0;
    }


    @Override
    public String getDigest() {
        return null;
    }


    @Override
    public RelTraitSet getTraitSet() {
        RelTraitSet set = RelTraitSet.createEmpty();
        set.add( Convention.NONE );
        return set;
    }


    @Override
    public String getDescription() {
        return null;
    }


    @Override
    public RelOptCluster getCluster() {
        return RelOptCluster.create( new VolcanoPlanner(), new RexBuilder( new JavaTypeFactoryImpl() ) );
    }


    @Override
    public List<RexNode> getChildExps() {
        return new ArrayList<>();
    }


    @Override
    public Convention getConvention() {
        return null;
    }


    @Override
    public String getCorrelVariable() {
        return null;
    }


    @Override
    public RelNode getInput( int i ) {
        return null;
    }


    @Override
    public RelDataType getRowType() {
        List<RelDataTypeField> fields = new ArrayList<>();
        RelRecordType type = new RelRecordType( fields );
        return type;
    }


    @Override
    public RelDataType getExpectedInputRowType( int ordinalInParent ) {
        return null;
    }


    @Override
    public List<RelNode> getInputs() {
        return new ArrayList<>();
    }


    @Override
    public double estimateRowCount( RelMetadataQuery mq ) {
        return 0;
    }


    @Override
    public Set<String> getVariablesStopped() {
        return null;
    }


    @Override
    public Set<CorrelationId> getVariablesSet() {
        return null;
    }


    @Override
    public void collectVariablesUsed( Set<CorrelationId> variableSet ) {

    }


    @Override
    public void collectVariablesSet( Set<CorrelationId> variableSet ) {

    }


    @Override
    public void childrenAccept( RelVisitor visitor ) {

    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return null;
    }


    @Override
    public <M extends Metadata> M metadata( Class<M> metadataClass, RelMetadataQuery mq ) {
        return null;
    }


    @Override
    public void explain( RelWriter pw ) {

    }


    @Override
    public RelNode onRegister( RelOptPlanner planner ) {
        return null;
    }


    @Override
    public String recomputeDigest() {
        return null;
    }


    @Override
    public void replaceInput( int ordinalInParent, RelNode p ) {

    }


    @Override
    public RelOptTable getTable() {
        return null;
    }


    @Override
    public String getRelTypeName() {
        return null;
    }


    @Override
    public boolean isValid( Litmus litmus, Context context ) {
        return false;
    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return null;
    }


    @Override
    public void register( RelOptPlanner planner ) {

    }


    @Override
    public RelNode accept( RelShuttle shuttle ) {
        return null;
    }


    @Override
    public RelNode accept( RexShuttle shuttle ) {
        return null;
    }


    @Override
    public String relCompareString() {
        return null;
    }

}
