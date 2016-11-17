package io.digdag.plugin.gcp;

import io.digdag.client.config.Config;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;

import java.nio.file.Path;

abstract class BaseGcsOperator
        extends BaseGcpOperator
{
    private final GcsDownloadClient.Factory clientFactory;

    protected final Config params;

    protected BaseGcsOperator(Path projectPath, TaskRequest request, GcsDownloadClient.Factory clientFactory, GcpCredentialProvider credentialProvider)
    {
        super(projectPath, request, credentialProvider);
        this.clientFactory = clientFactory;
        this.params = request.getConfig()
                .mergeDefault(request.getConfig().getNestedOrGetEmpty("gcs"));
    }

    @Override
    protected TaskResult run(TaskExecutionContext ctx, GcpCredential credential, String projectId)
    {
        try (GcsDownloadClient bq = clientFactory.create(credential.credential())) {
            return run(ctx, bq, projectId);
        }
    }

    protected abstract TaskResult run(TaskExecutionContext ctx, GcsDownloadClient gcs, String projectId);
}
