package org.sagebionetworks.bridge.addf.azure;

/**
 * ADDF §4.4 — supplies the Azure Blob staging-container endpoint and credential for {@link BlobTransport}. Kept as an
 * interface deliberately: today there is one implementation ({@link SasBlobCredentialProvider}, SAS token), and a
 * future <b>service-principal</b> swap is a single new class with no caller change (index — auth decision). The
 * interface is transport-agnostic — it names <em>where</em> and <em>with what credential</em>, not <em>how</em> bytes
 * move.
 */
public interface BlobCredentialProvider {
    /** The staging container endpoint, e.g. {@code https://<account>.blob.core.windows.net/<container>} (no SAS). */
    String getContainerUrl();

    /** The SAS query string granting write access to the container (with or without a leading {@code ?}). */
    String getSasToken();
}
