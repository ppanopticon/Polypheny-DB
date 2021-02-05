package org.polypheny.db.adapter.mongodb;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
public class MongoDBStore extends DataStore {

    @SuppressWarnings("WeakerAccess")
    public static final String ADAPTER_NAME = "MongoDB";
    @SuppressWarnings("WeakerAccess")
    public static final String DESCRIPTION = "MongoDB is a document-based and distributed database. ";
    @SuppressWarnings("WeakerAccess")
    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new AdapterSettingList("type", false, true, false, ImmutableList.of("Mongo", "Fongo(embedded)")),
            new AdapterSettingString("host", false, true, false, "localhost"),
            new AdapterSettingInteger("port", false, true, false, 27017)
    );

    private DockerClient client;

    public MongoDBStore(int adapterId, String uniqueName, Map<String, String> settings) {
        super(adapterId, uniqueName, settings, false);
        DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                .withDockerHost("tcp://localhost:2375")
                .build();

        DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                .dockerHost(config.getDockerHost())
                .build();

        client = DockerClientImpl.getInstance(config, httpClient);
        client.pingCmd().exec();

        PullImageResultCallback callback = new PullImageResultCallback();

        client.pullImageCmd("mongo:latest").exec(callback);
        try {
            // TODO: blocking for now, maybe change or show warning?
            callback.awaitCompletion();
            log.warn("finished");
            client.createContainerCmd("mongo")
                    .withName(uniqueName)
                    .withExposedPorts(ExposedPort.parse(settings.get("port")))
                    .exec();
            client.startContainerCmd(uniqueName).exec();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
        return AVAILABLE_SETTINGS;
    }

    @Override
    public void shutdown() {
        client.stopContainerCmd(getUniqueName()).exec();
        client.removeContainerCmd(getUniqueName()).exec();
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
