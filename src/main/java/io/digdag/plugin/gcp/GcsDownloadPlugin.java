package io.digdag.plugin.gcp;

import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import io.digdag.spi.Plugin;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorProvider;

public class GcsDownloadPlugin
        implements Plugin
{
    @Override
    public <T> Class<? extends T> getServiceProvider(Class<T> type)
    {
        if (type == OperatorProvider.class) {
            return GcsDownloadOperatorProvider.class.asSubclass(type);
        }
        else {
            return null;
        }
    }

    private static class GcsDownloadOperatorProvider
            implements OperatorProvider
    {
        @Inject
        GcsDownloadClient.Factory clientFactory;
        @Inject
        GcpCredentialProvider credentialProvider;

        @Override
        public List<OperatorFactory> get()
        {
            return Collections.singletonList(
                    new GcsDownloadOperatorFactory(clientFactory, credentialProvider));
        }
    }
}