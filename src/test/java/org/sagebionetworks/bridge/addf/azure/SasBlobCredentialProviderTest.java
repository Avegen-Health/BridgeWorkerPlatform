package org.sagebionetworks.bridge.addf.azure;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;

public class SasBlobCredentialProviderTest {
    @Test
    public void readsContainerUrlAndSasFromConfig() {
        Config config = mock(Config.class);
        when(config.get("addf.azure.container.url")).thenReturn("https://acct.blob.core.windows.net/addf-export");
        when(config.get("addf.azure.sas.token")).thenReturn("?sig=abc&se=2027-01-01");

        SasBlobCredentialProvider provider = new SasBlobCredentialProvider();
        provider.setBridgeConfig(config);

        assertEquals(provider.getContainerUrl(), "https://acct.blob.core.windows.net/addf-export");
        assertEquals(provider.getSasToken(), "?sig=abc&se=2027-01-01");
    }
}
