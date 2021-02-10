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

package org.polypheny.db.processing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.util.Casing;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationQueryPlan;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.mql.MqlCreateDatabase;
import org.polypheny.db.mql.MqlExecutableStatement;
import org.polypheny.db.mql.MqlFind;
import org.polypheny.db.mql.MqlNode;
import org.polypheny.db.mql2rel.MqlToRelConverter;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.prepare.PolyphenyDbSqlValidator;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rel.type.RelRecordType;
import org.polypheny.db.rel.type.StructKind;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.sql.SqlExplainFormat;
import org.polypheny.db.sql.SqlExplainLevel;
import org.polypheny.db.sql.parser.SqlParser;
import org.polypheny.db.sql.parser.SqlParser.SqlParserConfig;
import org.polypheny.db.sql2rel.RelDecorrelator;
import org.polypheny.db.sql2rel.SqlToRelConverter;
import org.polypheny.db.sql2rel.StandardConvertletTable;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.DeadlockException;
import org.polypheny.db.transaction.Lock;
import org.polypheny.db.transaction.LockManager;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionImpl;
import org.polypheny.db.util.Pair;


/**
 * Testing basic functionality of processing Mql TODO DL
 */
@Slf4j
public class MqlProcessorImpl implements MqlProcessor, RelOptTable.ViewExpander {

    private static final SqlParserConfig parserConfig;
    private PolyphenyDbSqlValidator validator;


    static {
        SqlParser.ConfigBuilder configConfigBuilder = SqlParser.configBuilder();
        configConfigBuilder.setCaseSensitive( RuntimeConfig.CASE_SENSITIVE.getBoolean() );
        configConfigBuilder.setUnquotedCasing( Casing.TO_LOWER );
        configConfigBuilder.setQuotedCasing( Casing.TO_LOWER );
        parserConfig = configConfigBuilder.build();
    }


    @Override
    public RelRoot expandView( RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath ) {
        return null;
    }


    @Override
    public MqlNode parse( String mql ) {
        if( mql.contains( "find" )){
            return new MqlFind(mql);
        }else {
            // createDatabase
            return new MqlCreateDatabase( mql, mql.split( " " )[1] );
        }
    }


    @Override
    public Pair<MqlNode, RelDataType> validate( Transaction transaction, MqlNode parsed, boolean addDefaultValues ) {
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Validating SQL ..." );
        }
        stopWatch.start();

        MqlNode validated;
        RelDataType type;

        validated = parsed;
        List<RelDataTypeField> fields = new ArrayList<>();
        type = new RelRecordType( StructKind.FULLY_QUALIFIED, fields );

        stopWatch.stop();

        return new Pair<>( validated, type );

    }


    @Override
    public RelRoot translate( Statement statement, MqlNode mql ) {
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Planning Statement ..." );
        }
        stopWatch.start();

        final RexBuilder rexBuilder = new RexBuilder( statement.getTransaction().getTypeFactory() );

        final RelOptCluster cluster = RelOptCluster.create( statement.getQueryProcessor().getPlanner(), rexBuilder );

        final MqlToRelConverter mqlToRelConverter = new MqlToRelConverter( this, validator, statement.getTransaction().getCatalogReader(), cluster, StandardConvertletTable.INSTANCE );
        RelRoot logicalRoot = mqlToRelConverter.convertQuery( mql, statement, true );

        if ( statement.getTransaction().isAnalyze() ) {
            InformationManager queryAnalyzer = statement.getTransaction().getQueryAnalyzer();
            InformationPage page = new InformationPage( "Logical Query Plan" ).setLabel( "plans" );
            page.fullWidth();
            InformationGroup group = new InformationGroup( page, "Logical Query Plan" );
            queryAnalyzer.addPage( page );
            queryAnalyzer.addGroup( group );
            InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                    group,
                    RelOptUtil.dumpPlan( "Logical Query Plan", logicalRoot.rel, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
            queryAnalyzer.registerInformation( informationQueryPlan );
        }


        return logicalRoot;
    }


    @Override
    public PolyphenyDbSignature<?> prepareDdl( Statement statement, MqlNode parsed ) {
        if ( parsed instanceof MqlExecutableStatement ) {
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
            throw new RuntimeException( "All DDL queries should be of a type that inherits MqlExecutableStatement. But this one is of type " + parsed.getClass() );
        }
    }

}
