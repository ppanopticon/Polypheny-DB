package org.polypheny.db.adapter.mongodb;

import com.google.common.collect.ImmutableList;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;

import java.util.List;
import java.util.Map;

public class MongoDBStore extends DataStore {

    @SuppressWarnings("WeakerAccess")
    public static final String ADAPTER_NAME = "MongoDB";
    @SuppressWarnings("WeakerAccess")
    public static final String DESCRIPTION = "MongoDB is a document-based and distributed database. ";
    @SuppressWarnings("WeakerAccess")
    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new AdapterSettingList( "type", false, true, false, ImmutableList.of( "Mongo", "Fongo(embedded)" ) ),
            new AdapterSettingString( "host", false, true, false, "localhost" ),
            new AdapterSettingInteger( "port", false, true, false, 27017 )
    );

    public MongoDBStore(int adapterId, String uniqueName, Map<String, String> settings, boolean persistent) {
        super(adapterId, uniqueName, settings, persistent);
    }

    @Override
    public String getAdapterName() {
        return ADAPTER_NAME;
    }

    @Override
    public void createNewSchema(SchemaPlus rootSchema, String name) {

    }

    @Override
    public Table createTableSchema(CatalogTable combinedTable, List<CatalogColumnPlacement> columnPlacementsOnStore) {
        return null;
    }

    @Override
    public Schema getCurrentSchema() {
        return null;
    }

    @Override
    public void truncate(Context context, CatalogTable table) {

    }

    @Override
    public boolean prepare(PolyXid xid) {
        return false;
    }

    @Override
    public void commit(PolyXid xid) {

    }

    @Override
    public void rollback(PolyXid xid) {

    }

    @Override
    public List<AdapterSetting> getAvailableSettings() {
        return null;
    }

    @Override
    public void shutdown() {

    }

    @Override
    protected void reloadSettings(List<String> updatedSettings) {

    }

    @Override
    public void createTable(Context context, CatalogTable combinedTable) {

    }

    @Override
    public void dropTable(Context context, CatalogTable combinedTable) {

    }

    @Override
    public void addColumn(Context context, CatalogTable catalogTable, CatalogColumn catalogColumn) {

    }

    @Override
    public void dropColumn(Context context, CatalogColumnPlacement columnPlacement) {

    }

    @Override
    public void addIndex(Context context, CatalogIndex catalogIndex) {

    }

    @Override
    public void dropIndex(Context context, CatalogIndex catalogIndex) {

    }

    @Override
    public void updateColumnType(Context context, CatalogColumnPlacement columnPlacement, CatalogColumn catalogColumn) {

    }

    @Override
    public List<AvailableIndexMethod> getAvailableIndexMethods() {
        return null;
    }

    @Override
    public AvailableIndexMethod getDefaultIndexMethod() {
        return null;
    }

    @Override
    public List<FunctionalIndexInfo> getFunctionalIndexes(CatalogTable catalogTable) {
        return null;
    }
}
