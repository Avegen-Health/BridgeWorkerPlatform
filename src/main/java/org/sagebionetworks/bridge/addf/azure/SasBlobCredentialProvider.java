package org.sagebionetworks.bridge.addf.azure;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;

/**
 * ADDF §4.4 — the SAS-token implementation of {@link BlobCredentialProvider}. Reads the (non-secret) container URL and
 * the (secret) SAS token from config. In dev/uat/prod both are overridden at runtime by EB environment properties
 * sourced from SSM at deploy ({@code /bridgeworker-<env>/AddfBlobSasToken}, wired {@code NoEcho}); the {@code .conf}
 * default is a blank placeholder, so a misconfigured environment fails loudly at publish rather than silently reading
 * a stale token.
 */
@Component
public class SasBlobCredentialProvider implements BlobCredentialProvider {
    static final String CONFIG_KEY_CONTAINER_URL = "addf.azure.container.url";
    static final String CONFIG_KEY_SAS_TOKEN = "addf.azure.sas.token";

    private String containerUrl;
    private String sasToken;

    @Autowired
    public final void setBridgeConfig(Config config) {
        this.containerUrl = config.get(CONFIG_KEY_CONTAINER_URL);
        this.sasToken = config.get(CONFIG_KEY_SAS_TOKEN);
    }

    @Override
    public String getContainerUrl() {
        return containerUrl;
    }

    @Override
    public String getSasToken() {
        return sasToken;
    }
}
