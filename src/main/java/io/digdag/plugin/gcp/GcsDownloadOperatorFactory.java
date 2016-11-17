package io.digdag.plugin.gcp;


import com.google.api.services.storage.Storage;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.*;
import io.digdag.standards.operator.state.TaskState;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;

public class GcsDownloadOperatorFactory implements OperatorFactory {
    private final GcsDownloadClient.Factory clientFactory;
    private final GcpCredentialProvider credentialProvider;

    @Inject
    GcsDownloadOperatorFactory(
            GcsDownloadClient.Factory clientFactory,
            GcpCredentialProvider credentialProvider)
    {
        this.clientFactory = clientFactory;
        this.credentialProvider = credentialProvider;
    }
    @Override
    public String getType() {
        return "gcs_download";
    }

    @Override
    public Operator newOperator(Path projectPath, TaskRequest request) {
        return new GcsDownloadOperator(projectPath, request);
    }

    private class GcsDownloadOperator
            extends BaseGcsOperator
    {
        private final TaskState state;

        GcsDownloadOperator(Path projectPath, TaskRequest request)
        {
            super(projectPath, request, clientFactory, credentialProvider);
            this.state = TaskState.of(request);
        }

        @Override
        protected TaskResult run(TaskExecutionContext ctx, GcsDownloadClient gcsDownloadClient, String projectId)
        {
            Optional<String> bucket = params.getOptional("bucket", String.class);
            Optional<String> prefix = params.getOptional("prefix", String.class);
            Optional<String> outFile = params.getOptional("out_file", String.class);

            if (!bucket.isPresent() || !prefix.isPresent()) {
                throw new ConfigException("Either the gcp_download operator both 'bucket' and 'prefix' parameters must be set");
            }

            try(FileOutputStream fileOutputStream = new FileOutputStream(outFile.get())) {
                gcsDownloadClient.list(bucket.get(), prefix.get()).orElseThrow(RuntimeException::new).getItems().forEach(s -> {
                    try {
                        Storage.Objects.Get getObject = gcsDownloadClient.download(s.getBucket(), s.getName()).orElseThrow(RuntimeException::new);
                        getObject.executeMediaAndDownloadTo(fileOutputStream);
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                });
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
            return TaskResult.empty(request);
        }
    }
}
