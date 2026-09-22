package org.sagebionetworks.bridge.addf.azure;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import com.azure.storage.blob.BlobContainerClient;
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

    /**
     * A transport whose real SDK call ({@code doUpload}) is scripted to fail a set number of times before succeeding,
     * with a no-op backoff and a stubbed container client so no live Azure container is needed.
     */
    private static BlobTransport flakyTransport(AtomicInteger calls, int failuresBeforeSuccess) {
        return new BlobTransport() {
            @Override
            BlobContainerClient buildContainerClient() {
                return null; // bypass the SAS/placeholder guards — this test targets the retry loop
            }

            @Override
            void doUpload(BlobContainerClient containerClient, PublishedBlob blob) {
                if (calls.getAndIncrement() < failuresBeforeSuccess) {
                    throw new RuntimeException("transient blob error");
                }
            }

            @Override
            void sleepBackoff(int attempt) {
                // no real sleep in tests
            }
        };
    }

    @Test
    public void retriesTransientFailureThenSucceeds() {
        AtomicInteger calls = new AtomicInteger();
        BlobTransport flaky = flakyTransport(calls, 2); // fail twice, succeed on the third attempt
        flaky.upload(ImmutableList.of(new PublishedBlob("biaffect-3/current/tables/phq9.parquet",
                new File("phq9.parquet"))));
        assertEquals(calls.get(), 3);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void exhaustsRetriesThenThrows() {
        AtomicInteger calls = new AtomicInteger();
        BlobTransport flaky = flakyTransport(calls, Integer.MAX_VALUE); // always fail
        try {
            flaky.upload(ImmutableList.of(new PublishedBlob("biaffect-3/current/tables/phq9.parquet",
                    new File("phq9.parquet"))));
        } finally {
            assertEquals(calls.get(), BlobTransport.MAX_ATTEMPTS);
        }
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
