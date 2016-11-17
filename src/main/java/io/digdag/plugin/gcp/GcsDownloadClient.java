package io.digdag.plugin.gcp;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.treasuredata.client.ProxyConfig;
import io.digdag.core.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

class GcsDownloadClient
        extends BaseGcpClient<Storage>
{
    private static Logger logger = LoggerFactory.getLogger(GcsDownloadClient.class);
    GcsDownloadClient(GoogleCredential credential, Optional<ProxyConfig> proxyConfig)
    {
        super(credential, proxyConfig);
    }

    @Override
    protected Storage client(GoogleCredential credential, HttpTransport transport, JsonFactory jsonFactory)
    {
        if (credential.createScopedRequired()) {
            credential = credential.createScoped(BigqueryScopes.all());
        }

        return new Storage.Builder(transport, jsonFactory, credential)
                .setApplicationName("Digdag")
                .build();
    }

    java.util.Optional<Objects> list(String bucket, String prefix)
            throws IOException
    {
        try {
            return java.util.Optional.of(client.objects()
                    .list(bucket)
                    .setPrefix(prefix)
                    .execute());
        }
        catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
                return java.util.Optional.empty();
            }
            throw e;
        }
    }

    java.util.Optional<Storage.Objects.Get> download(String bucket, String name)
            throws IOException
    {
        try {
            return java.util.Optional.of(client.objects()
                    .get(bucket, name));

        }
        catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
                return java.util.Optional.empty();
            }
            throw e;
        }
    }

    static class Factory
            extends BaseGcpClient.Factory
    {
        @Inject
        public Factory(@Environment Map<String, String> environment)
        {
            super(environment);
        }

        GcsDownloadClient create(GoogleCredential credential)
        {
            return new GcsDownloadClient(credential, proxyConfig);
        }
    }
}