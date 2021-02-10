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

import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlExecutableStatement;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.sql.util.SqlVisitor;
import org.polypheny.db.sql.validate.SqlValidator;
import org.polypheny.db.sql.validate.SqlValidatorScope;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Litmus;

public class MqlCreateCollection extends MqlCreate implements SqlExecutableStatement {

    /**
     * Creates a node.
     */
    MqlCreateCollection() {
    }


    @Override
    public MqlNode clone( SqlParserPos pos ) {
        return null;
    }


    @Override
    public String toString() {
        return null;
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {

    }


    @Override
    public void validate( SqlValidator validator, SqlValidatorScope scope ) {

    }


    @Override
    public <R> R accept( SqlVisitor<R> visitor ) {
        return null;
    }


    @Override
    public boolean equalsDeep( MqlNode node, Litmus litmus ) {
        return false;
    }


    @Override
    public void execute( Context context, Statement statement ) {

    }

}
