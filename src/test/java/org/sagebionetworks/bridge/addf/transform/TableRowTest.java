package org.sagebionetworks.bridge.addf.transform;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import org.testng.annotations.Test;

public class TableRowTest {
    @Test
    public void carriesTableAndKey() {
        TableRow row = new TableRow("phq9", "rec-1");
        assertEquals(row.getTable(), "phq9");
        assertEquals(row.getKey(), "rec-1");
    }

    @Test
    public void putIsChainableAndGettable() {
        TableRow row = new TableRow("phq9", "rec-1");
        TableRow returned = row.put("a", 1).put("b", "two");
        assertSame(returned, row);
        assertEquals(row.get("a"), 1);
        assertEquals(row.get("b"), "two");
        assertNull(row.get("missing"));
    }

    @Test
    public void preservesInsertionOrder() {
        TableRow row = new TableRow("t", "k");
        row.put("z", 1).put("a", 2).put("m", 3);
        assertEquals(row.getValues().keySet().toString(), "[z, a, m]");
    }
}
