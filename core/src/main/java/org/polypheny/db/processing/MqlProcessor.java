package org.polypheny.db.processing;

import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.mql.MqlNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.Pair;

public interface MqlProcessor {
    MqlNode parse(String mql );

    // Pair<SqlNode, RelDataType> validate(Transaction transaction, SqlNode parsed, boolean addDefaultValues );

    // RelRoot translate(Statement statement, SqlNode sql );

    PolyphenyDbSignature<?> prepareDdl(Statement statement, MqlNode parsed );

    // RelDataType getParameterRowType( SqlNode left );
}
