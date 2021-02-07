package org.polypheny.db.mql;

import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.sql.SqlExecutableStatement;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.sql.util.SqlVisitor;
import org.polypheny.db.sql.validate.SqlValidator;
import org.polypheny.db.sql.validate.SqlValidatorScope;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Litmus;

public class MqlCreateDatabase extends MqlCreate implements MqlExecutableStatement {

    String store = "test";

    /**
     * Temporary constructor to test generation of Database TODO DL: remove later?
     */
    public MqlCreateDatabase() {
    }

    @Override
    public MqlNode clone(SqlParserPos pos) {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {

    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {

    }

    @Override
    public <R> R accept(SqlVisitor<R> visitor) {
        return null;
    }

    @Override
    public boolean equalsDeep(MqlNode node, Litmus litmus) {
        return false;
    }

    @Override
    public void execute(Context context, Statement statement) {
        Catalog catalog = Catalog.getInstance();

        Adapter adapterInstance = AdapterManager.getInstance().getAdapter( store );
        DataStore store = (DataStore) adapterInstance;
        String name = "test_database";

        catalog.addSchema(name, context.getDatabaseId(),
                context.getCurrentUserId(), Catalog.SchemaType.DOCUMENT);

        store.createNewSchema(null, name);
    }
}
