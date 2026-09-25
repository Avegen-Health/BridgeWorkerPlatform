package org.sagebionetworks.bridge.addf.azure;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.azure.storage.blob.BlobContainerClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Building the Azure container client — the step between "we have credentials in SSM" and "bytes leave the VPC".
 * Its guards are the last thing standing between a misconfigured environment and a publish that appears to succeed
 * while writing nowhere, so they are asserted here rather than left to the first real deploy.
 */
public class BlobContainerClientBuildTest {
    private static final String CONTAINER_URL = "https://gvbridgeaddfexportuat.blob.core.windows.net/addf-export";
    private static final String SAS = "sv=2022-11-02&ss=b&srt=co&sp=rwlac&sig=fake-signature";

    private BlobCredentialProvider mockProvider;
    private BlobTransport transport;

    @BeforeMethod
    public void before() {
        mockProvider = mock(BlobCredentialProvider.class);
        transport = new BlobTransport();
        transport.setBlobCredentialProvider(mockProvider);
    }

    @Test
    public void aValidContainerUrlAndSasProduceAClient() {
        when(mockProvider.getContainerUrl()).thenReturn(CONTAINER_URL);
        when(mockProvider.getSasToken()).thenReturn(SAS);

        BlobContainerClient client = transport.buildContainerClient();

        assertNotNull(client);
    }

    @Test
    public void aSasTokenStoredWithItsLeadingQuestionMarkIsAccepted() {
        // SAS tokens are routinely copied out of the Azure portal complete with the leading '?'. Rejecting or
        // mis-parsing that form would fail authentication at upload time with an error that points at the
        // credentials rather than at the copy-paste, so the transport tolerates both spellings.
        when(mockProvider.getContainerUrl()).thenReturn(CONTAINER_URL);
        when(mockProvider.getSasToken()).thenReturn("?" + SAS);

        BlobContainerClient withQuestionMark = transport.buildContainerClient();

        when(mockProvider.getSasToken()).thenReturn(SAS);
        BlobContainerClient without = transport.buildContainerClient();

        assertNotNull(withQuestionMark);
        assertNotNull(without);
        // Same container either way — the '?' is stripped, not treated as part of the signature.
        org.testng.Assert.assertEquals(withQuestionMark.getBlobContainerName(), without.getBlobContainerName());
    }

    @Test
    public void aPlaceholderContainerUrlBlocksPublishRatherThanFailingLater() {
        // The .conf ships PLACEHOLDER_BLOCKED_ON_ADDI as the default. Building a client against it would produce a
        // client that fails per-blob deep inside the upload loop; failing here names the actual problem.
        when(mockProvider.getContainerUrl()).thenReturn("PLACEHOLDER_BLOCKED_ON_ADDI");
        when(mockProvider.getSasToken()).thenReturn(SAS);

        try {
            transport.buildContainerClient();
            org.testng.Assert.fail("expected the placeholder to block");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("placeholder"), expected.getMessage());
        }
    }

    @Test
    public void anEmptyOrNullContainerUrlBlocks() {
        when(mockProvider.getSasToken()).thenReturn(SAS);
        for (String url : new String[] { null, "" }) {
            when(mockProvider.getContainerUrl()).thenReturn(url);
            try {
                transport.buildContainerClient();
                org.testng.Assert.fail("expected an unset container URL to block, url=" + url);
            } catch (IllegalStateException expected) {
                assertTrue(expected.getMessage().contains("container URL"), expected.getMessage());
            }
        }
    }

    @Test
    public void anEmptyOrNullSasBlocks() {
        when(mockProvider.getContainerUrl()).thenReturn(CONTAINER_URL);
        for (String sas : new String[] { null, "" }) {
            when(mockProvider.getSasToken()).thenReturn(sas);
            try {
                transport.buildContainerClient();
                org.testng.Assert.fail("expected an unset SAS to block, sas=" + sas);
            } catch (IllegalStateException expected) {
                assertTrue(expected.getMessage().contains("SAS token"), expected.getMessage());
            }
        }
    }

    @Test
    public void backoffSleepsAndReturnsNormally() {
        // The retry loop's only side effect between attempts. Asserting it actually waits keeps a future "optimise
        // the retries" change from turning a transient-blip absorber into a three-shot burst.
        long before = System.nanoTime();
        transport.sleepBackoff(1);
        long elapsedMillis = (System.nanoTime() - before) / 1_000_000L;

        assertTrue(elapsedMillis >= BlobTransport.BACKOFF_MILLIS - 50,
                "expected at least " + BlobTransport.BACKOFF_MILLIS + "ms, waited " + elapsedMillis);
    }
}
