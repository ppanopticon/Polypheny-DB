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

package org.polypheny.db.adapter.enumerable;


import java.util.function.Predicate;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.convert.ConverterRule;
import org.polypheny.db.rel.core.Calc;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.logical.LogicalCalc;


/**
 * Rule to convert a {@link LogicalCalc} to an {@link EnumerableCalc}.
 */
class EnumerableCalcRule extends ConverterRule {

    EnumerableCalcRule() {
        // The predicate ensures that if there's a multiset, FarragoMultisetSplitter will work on it first.
        super( LogicalCalc.class, (Predicate<Calc>) RelOptUtil::containsMultisetOrWindowedAgg, Convention.NONE, EnumerableConvention.INSTANCE, RelFactories.LOGICAL_BUILDER, "EnumerableCalcRule" );
    }


    @Override
    public RelNode convert( RelNode rel ) {
        final LogicalCalc calc = (LogicalCalc) rel;
        final RelNode input = calc.getInput();
        return EnumerableCalc.create( convert( input, input.getTraitSet().replace( EnumerableConvention.INSTANCE ) ), calc.getProgram() );
    }
}

