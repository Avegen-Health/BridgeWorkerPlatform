package org.sagebionetworks.bridge.addf.publish;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import java.io.File;

import org.testng.annotations.Test;

public class PublishedBlobTest {
    @Test
    public void holdsKeyAndFile() {
        File file = new File("phq9.parquet");
        PublishedBlob blob = new PublishedBlob("biaffect-3/current/tables/phq9.parquet", file);
        assertEquals(blob.getKey(), "biaffect-3/current/tables/phq9.parquet");
        assertSame(blob.getLocalFile(), file);
    }
}
