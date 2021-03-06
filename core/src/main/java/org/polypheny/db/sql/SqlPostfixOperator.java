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

package org.polypheny.db.sql;


import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.validate.SqlValidator;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Util;


/**
 * A postfix unary operator.
 */
public class SqlPostfixOperator extends SqlOperator {


    public SqlPostfixOperator( String name, SqlKind kind, int prec, PolyReturnTypeInference returnTypeInference, PolyOperandTypeInference operandTypeInference, PolyOperandTypeChecker operandTypeChecker ) {
        super(
                name,
                kind,
                leftPrec( prec, true ),
                rightPrec( prec, true ),
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker );
    }


    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.POSTFIX;
    }


    @Override
    public String getSignatureTemplate( final int operandsCount ) {
        Util.discard( operandsCount );
        return "{1} {0}";
    }


    @Override
    protected RelDataType adjustType( SqlValidator validator, SqlCall call, RelDataType type ) {
        if ( PolyTypeUtil.inCharFamily( type ) ) {
            // Determine coercibility and resulting collation name of unary operator if needed.
            RelDataType operandType = validator.getValidatedNodeType( call.operand( 0 ) );
            if ( null == operandType ) {
                throw new AssertionError( "operand's type should have been derived" );
            }
            if ( PolyTypeUtil.inCharFamily( operandType ) ) {
                SqlCollation collation = operandType.getCollation();
                assert null != collation : "An implicit or explicit collation should have been set";
                type = validator.getTypeFactory()
                        .createTypeWithCharsetAndCollation( type, type.getCharset(), collation );
            }
        }
        return type;
    }


    @Override
    public boolean validRexOperands( int count, Litmus litmus ) {
        if ( count != 1 ) {
            return litmus.fail( "wrong operand count {} for {}", count, this );
        }
        return litmus.succeed();
    }
}

