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

package org.polypheny.db.adapter.cottontail.rules;


import org.polypheny.db.adapter.cottontail.CottontailConvention;
import org.polypheny.db.adapter.cottontail.CottontailTable;
import org.polypheny.db.adapter.cottontail.rel.CottontailTableModify;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.core.TableModify.Operation;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.tools.RelBuilderFactory;


public class CottontailTableModificationRule extends CottontailConverterRule {

    CottontailTableModificationRule( CottontailConvention out, RelBuilderFactory relBuilderFactory ) {
        super( TableModify.class, r -> true, Convention.NONE, out, relBuilderFactory, "CottontailTableModificationRule:" + out.getName() );
    }


    @Override
    public boolean matches( RelOptRuleCall call ) {
        final TableModify tableModify = call.rel( 0 );
        if ( tableModify.getTable().unwrap( CottontailTable.class ) == null ) {
            return false;
        }

        if ( !tableModify.getTable().unwrap( CottontailTable.class ).getUnderlyingConvention().equals( this.out ) ) {
            return false;
        }
        return tableModify.getOperation() != Operation.MERGE;
    }


    @Override
    public RelNode convert( RelNode rel ) {
        final TableModify modify = (TableModify) rel;

        final ModifiableTable modifiableTable = modify.getTable().unwrap( ModifiableTable.class );

        if ( modifiableTable == null ) {
            return null;
        }
        if ( modify.getTable().unwrap( CottontailTable.class ) == null ) {
            return null;
        }

        final RelTraitSet traitSet = modify.getTraitSet().replace( out );

        return new CottontailTableModify(
                modify.getCluster(),
                traitSet,
                modify.getTable(),
                modify.getCatalogReader(),
                RelOptRule.convert( modify.getInput(), traitSet ),
                modify.getOperation(),
                modify.getUpdateColumnList(),
                modify.getSourceExpressionList(),
                modify.isFlattened()
        );
    }

}
