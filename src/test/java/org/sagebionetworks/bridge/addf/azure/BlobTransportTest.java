package org.sagebionetworks.bridge.addf.azure;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.addf.publish.PublishedBlob;

public class BlobTransportTest {
    private BlobCredentialProvider mockProvider;
    private BlobTransport transport;

    @BeforeMethod
    public void before() {
        mockProvider = mock(BlobCredentialProvider.class);
        transport = new BlobTransport();
        transport.setBlobCredentialProvider(mockProvider);
    }

    @Test
    public void emptyDeltaUploadsNothing() {
        // Returns early before building any Azure client — must not touch the credential provider.
        transport.upload(ImmutableList.<PublishedBlob>of());
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void placeholderContainerUrlIsBlocked() {
        when(mockProvider.getContainerUrl())
                .thenReturn("https://acct.blob.core.windows.net/PLACEHOLDER_BLOCKED_ON_ADDI");
        when(mockProvider.getSasToken()).thenReturn("sig=abc");
        transport.upload(ImmutableList.of(new PublishedBlob("biaffect-3/current/tables/phq9.parquet",
                new File("phq9.parquet"))));
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void blankSasTokenIsBlocked() {
        when(mockProvider.getContainerUrl()).thenReturn("https://acct.blob.core.windows.net/addf-export");
        when(mockProvider.getSasToken()).thenReturn("");
        transport.upload(ImmutableList.of(new PublishedBlob("biaffect-3/current/tables/phq9.parquet",
                new File("phq9.parquet"))));
    }
}
