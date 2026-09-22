package org.sagebionetworks.bridge.addf.transform;

/** One column in an ADDF table: a name plus its FAIR logical {@link ColumnType}. */
public final class Column {
    private final String name;
    private final ColumnType type;

    public Column(String name, ColumnType type) {
        this.name = name;
        this.type = type;
    }

    public static Column of(String name, ColumnType type) {
        return new Column(name, type);
    }

    public String getName() {
        return name;
    }

    public ColumnType getType() {
        return type;
    }
}
