package org.polypheny.db.adapter.mongodb;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.exception.ConflictException;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.google.common.collect.ImmutableList;
import com.mongodb.MongoClientOptions;
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
import org.polypheny.db.util.FileSystemManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
public class MongoDBStore extends DataStore {

    @SuppressWarnings("WeakerAccess")
    public static final String ADAPTER_NAME = "MongoDB";
    @SuppressWarnings("WeakerAccess")
    public static final String DESCRIPTION = "MongoDB is a document-based and distributed database.";
    @SuppressWarnings("WeakerAccess")
    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new AdapterSettingBoolean("persistent", false, true, false, false),
            new AdapterSettingList("type", false, true, false, ImmutableList.of("Mongo", "Fongo(embedded)")),
            new AdapterSettingString("host", false, true, false, "localhost"),
            new AdapterSettingInteger("port", false, true, false, 27017)
    );

    private DockerClient client;
    private MongoSchema currentSchema;

    public MongoDBStore(int adapterId, String uniqueName, Map<String, String> settings) {
        super(adapterId, uniqueName, settings, Boolean.parseBoolean(settings.get("persistent")));
        FileSystemManager fileManger = FileSystemManager.getInstance();
        //String certPath = fileManger.registerNewFolder("certs").getAbsolutePath();

        // TODO: check if docker running or else do not show?
        DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                .withDockerHost("tcp://localhost:2375")
                //.withDockerTlsVerify(true)
                //.withDockerCertPath(certPath)
                .build();

        DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                .dockerHost(config.getDockerHost())
                .sslConfig(config.getSSLConfig())
                .build();

        client = DockerClientImpl.getInstance(config, httpClient);
        client.pingCmd().exec();

        PullImageResultCallback callback = new PullImageResultCallback();

        client.pullImageCmd("mongo:latest").exec(callback);
        try {
            // TODO: blocking for now, maybe change or show warning?
            callback.awaitCompletion();
            log.warn("finished");

            generateContainer(uniqueName, settings);
        } catch (InterruptedException | ConflictException e) {
            e.printStackTrace();
        }
    }

    private void generateContainer(String uniqueName, Map<String, String> settings) {
        /* TODO DL: solution when container name already exist
        if (client.listContainersCmd().exec().stream().anyMatch(container -> container.getNames()[0].contains(uniqueName))) {
            generateContainer(uniqueName+"_1", settings);
            return;
        }*/
        int port = Integer.parseInt(settings.get("port"));
        Ports bindings = new Ports();
        bindings.bind(ExposedPort.tcp(port), Ports.Binding.bindPort(port));
        CreateContainerCmd cmd = client.createContainerCmd("mongo")
                .withName(uniqueName)
                .withExposedPorts(ExposedPort.tcp(port));

        cmd.getHostConfig().withPortBindings(bindings);
        cmd.exec();

        client.startContainerCmd(uniqueName).exec();
    }


    @Override
    public String getAdapterName() {
        return ADAPTER_NAME;
    }

    @Override
    public void createNewSchema(SchemaPlus rootSchema, String name) {
        this.currentSchema = new MongoSchema("localhost", Integer.parseInt(settings.get("port")), "test_db");
    }

    @Override
    public Table createTableSchema(CatalogTable combinedTable, List<CatalogColumnPlacement> columnPlacementsOnStore) {
        return new MongoTable(combinedTable.name);
    }

    @Override
    public Schema getCurrentSchema() {
        return this.currentSchema;
    }

    @Override
    public void truncate(Context context, CatalogTable table) {

    }

    @Override
    public boolean prepare(PolyXid xid) {
        return true;
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
        this.currentSchema.mongoDb.createCollection(combinedTable.name);
    }

    @Override
    public void dropTable(Context context, CatalogTable combinedTable) {

    }

    @Override
    public void addColumn(Context context, CatalogTable catalogTable, CatalogColumn catalogColumn) {
        this.currentSchema.mongoDb.createCollection(catalogColumn.name);
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
