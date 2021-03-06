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

package org.polypheny.db.test;


import com.google.common.collect.ImmutableList;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.sql.SqlFunction;
import org.polypheny.db.sql.SqlFunctionCategory;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.SqlOperatorBinding;
import org.polypheny.db.sql.SqlOperatorTable;
import org.polypheny.db.sql.util.ChainedSqlOperatorTable;
import org.polypheny.db.sql.util.ListSqlOperatorTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.checker.OperandTypes;


/**
 * Mock operator table for testing purposes. Contains the standard SQL operator table, plus a list of operators.
 */
public class MockSqlOperatorTable extends ChainedSqlOperatorTable {

    private final ListSqlOperatorTable listOpTab;


    public MockSqlOperatorTable( SqlOperatorTable parentTable ) {
        super( ImmutableList.of( parentTable, new ListSqlOperatorTable() ) );
        listOpTab = (ListSqlOperatorTable) tableList.get( 1 );
    }


    /**
     * Adds an operator to this table.
     */
    public void addOperator( SqlOperator op ) {
        listOpTab.add( op );
    }


    public static void addRamp( MockSqlOperatorTable opTab ) {
        // Don't use anonymous inner classes. They can't be instantiated using reflection when we are deserializing from JSON.
        opTab.addOperator( new RampFunction() );
        opTab.addOperator( new DedupFunction() );
    }


    /**
     * "RAMP" user-defined function.
     */
    public static class RampFunction extends SqlFunction {

        public RampFunction() {
            super( "RAMP", SqlKind.OTHER_FUNCTION, null, null, OperandTypes.NUMERIC, SqlFunctionCategory.USER_DEFINED_FUNCTION );
        }


        @Override
        public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
            return typeFactory.builder()
                    .add( "I", null, PolyType.INTEGER )
                    .build();
        }
    }


    /**
     * "DEDUP" user-defined function.
     */
    public static class DedupFunction extends SqlFunction {

        public DedupFunction() {
            super( "DEDUP", SqlKind.OTHER_FUNCTION, null, null, OperandTypes.VARIADIC, SqlFunctionCategory.USER_DEFINED_FUNCTION );
        }


        @Override
        public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
            return typeFactory.builder()
                    .add( "NAME", null, PolyType.VARCHAR, 1024 )
                    .build();
        }
    }
}

