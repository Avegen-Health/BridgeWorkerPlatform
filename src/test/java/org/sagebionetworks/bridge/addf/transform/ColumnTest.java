package org.sagebionetworks.bridge.addf.transform;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class ColumnTest {
    @Test
    public void ofBuildsColumn() {
        Column c = Column.of("health_code", ColumnType.TEXT);
        assertEquals(c.getName(), "health_code");
        assertEquals(c.getType(), ColumnType.TEXT);
    }

    @Test
    public void constructorBuildsColumn() {
        Column c = new Column("participant_version", ColumnType.INTEGER);
        assertEquals(c.getName(), "participant_version");
        assertEquals(c.getType(), ColumnType.INTEGER);
    }

    @Test
    public void columnTypeValues() {
        // Exercises the enum's generated values()/valueOf().
        assertEquals(ColumnType.valueOf("DECIMAL"), ColumnType.DECIMAL);
        assertEquals(ColumnType.values().length, 5);
    }
}
