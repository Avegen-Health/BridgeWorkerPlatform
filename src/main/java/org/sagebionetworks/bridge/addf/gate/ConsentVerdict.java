package org.sagebionetworks.bridge.addf.gate;

/**
 * The result of {@link ConsentTestGate}. {@link #isShouldExport()} is the fail-closed decision; {@link #isTest()}
 * carries the {@code test_user} flag through to the {@code is_test} column (retained on the schema for
 * defence-in-depth even though test users are gated out of delivery).
 */
public class ConsentVerdict {
    private final boolean shouldExport;
    private final boolean test;

    private ConsentVerdict(boolean shouldExport, boolean test) {
        this.shouldExport = shouldExport;
        this.test = test;
    }

    /** Export this participant's data (currently sharing, not a test user, scope resolvable). */
    public static ConsentVerdict export(boolean isTest) {
        return new ConsentVerdict(true, isTest);
    }

    /** Do not export — NO_SHARING, test user, or indeterminate scope. */
    public static ConsentVerdict skip(boolean isTest) {
        return new ConsentVerdict(false, isTest);
    }

    public boolean isShouldExport() {
        return shouldExport;
    }

    public boolean isTest() {
        return test;
    }
}
