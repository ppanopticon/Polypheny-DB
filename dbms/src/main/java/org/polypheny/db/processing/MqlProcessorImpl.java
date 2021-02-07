package org.polypheny.db.processing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.avatica.Meta;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.mql.MqlCreateDatabase;
import org.polypheny.db.mql.MqlExecutableStatement;
import org.polypheny.db.mql.MqlNode;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.sql.SqlExecutableStatement;
import org.polypheny.db.sql.dialect.PolyphenyDbSqlDialect;
import org.polypheny.db.transaction.*;

import java.util.List;


/**
 * Testing basic functionality of processing Mql TODO DL
 */
public class MqlProcessorImpl implements MqlProcessor, RelOptTable.ViewExpander {
    @Override
    public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
        return null;
    }

    @Override
    public MqlNode parse(String mql) {
        return new MqlCreateDatabase();
    }

    @Override
    public PolyphenyDbSignature<?> prepareDdl(Statement statement, MqlNode parsed) {
        if (parsed instanceof MqlExecutableStatement) {
            try {
                // Acquire global schema lock
                LockManager.INSTANCE.lock( LockManager.GLOBAL_LOCK, (TransactionImpl) statement.getTransaction(), Lock.LockMode.EXCLUSIVE );
                // Execute statement
                ((MqlExecutableStatement) parsed).execute( statement.getPrepareContext(), statement );
                Catalog.getInstance().commit();
                return new PolyphenyDbSignature<>(
                        parsed.toString(),
                        ImmutableList.of(),
                        ImmutableMap.of(),
                        null,
                        ImmutableList.of(),
                        Meta.CursorFactory.OBJECT,
                        statement.getTransaction().getSchema(),
                        ImmutableList.of(),
                        -1,
                        null,
                        Meta.StatementType.OTHER_DDL,
                        new ExecutionTimeMonitor() );
            } catch ( DeadlockException e ) {
                throw new RuntimeException( "Exception while acquiring global schema lock", e );
            } catch ( NoTablePrimaryKeyException e ) {
                throw new RuntimeException( e );
            } finally {
                // Release lock
                // TODO: This can be removed when auto-commit of ddls is implemented
                LockManager.INSTANCE.unlock( LockManager.GLOBAL_LOCK, (TransactionImpl) statement.getTransaction() );
            }

        } else {
            throw new RuntimeException("All DDL queries should be of a type that inherits MqlExecutableStatement. But this one is of type " + parsed.getClass());
        }
    }
}
