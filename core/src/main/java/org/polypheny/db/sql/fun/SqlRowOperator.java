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

package org.polypheny.db.sql.fun;


import java.util.AbstractList;
import java.util.Map;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlCall;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperatorBinding;
import org.polypheny.db.sql.SqlSpecialOperator;
import org.polypheny.db.sql.SqlSyntax;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.InferTypes;
import org.polypheny.db.util.Pair;


/**
 * SqlRowOperator represents the special ROW constructor.
 *
 * TODO: describe usage for row-value construction and row-type construction (SQL supports both).
 */
public class SqlRowOperator extends SqlSpecialOperator {


    public SqlRowOperator( String name ) {
        super(
                name,
                SqlKind.ROW, MDX_PRECEDENCE,
                false,
                null,
                InferTypes.RETURN_TYPE,
                OperandTypes.VARIADIC );
        assert name.equals( "ROW" ) || name.equals( " " );
    }


    // implement SqlOperator
    @Override
    public SqlSyntax getSyntax() {
        // Function syntax would work too.
        return SqlSyntax.SPECIAL;
    }


    @Override
    public RelDataType inferReturnType( final SqlOperatorBinding opBinding ) {
        // The type of a ROW(e1,e2) expression is a record with the types {e1type,e2type}.  According to the standard, field names are implementation-defined.
        return opBinding.getTypeFactory().createStructType(
                new AbstractList<Map.Entry<String, RelDataType>>() {
                    @Override
                    public Map.Entry<String, RelDataType> get( int index ) {
                        return Pair.of(
                                SqlUtil.deriveAliasFromOrdinal( index ),
                                opBinding.getOperandType( index ) );
                    }


                    @Override
                    public int size() {
                        return opBinding.getOperandCount();
                    }
                } );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        SqlUtil.unparseFunctionSyntax( this, writer, call );
    }


    // override SqlOperator
    @Override
    public boolean requiresDecimalExpansion() {
        return false;
    }
}

