package org.sagebionetworks.bridge.addf.transform;

/**
 * Builds one flattened content row for a single assessment type. Implementations are Spring components registered by
 * the {@code item} identifier(s) they handle; {@link RecordFlattener} dispatches to them.
 */
public interface ActivityRowBuilder {
    /** The ADDF target table this builder writes (e.g. {@link AddfTables#PHQ9}). */
    String table();

    /** The assessment {@code item} identifiers this builder handles (as seen in info.json/metadata, case-insensitive). */
    String[] handledItems();

    /** Build the content row, or return null if the payload is missing/unusable (the manifest row is still emitted). */
    TableRow build(FlattenContext ctx);
}
