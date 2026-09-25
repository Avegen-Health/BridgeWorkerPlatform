package org.sagebionetworks.bridge.addf.transform;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Timestamp helpers for the ADDF contract.
 *
 * <p><b>Convention:</b> all {@code *_on} / {@code *_start} columns are stored as ISO-8601 <b>UTC</b> ({@code …Z}).
 * The activity tables additionally carry a {@code time_zone} column holding the original capture-time UTC offset
 * (e.g. {@code -04:00}) recovered from the assessment payload's own offset-bearing timestamp when one exists.</p>
 *
 * <p><b>Note on the data-contract prose vs. the golden data.</b> {@code data-contract.md §conventions} says
 * {@code time_zone} is "effectively Z today" because {@code clientTimeZone} on the record is usually null. However the
 * delivered golden previews show real offsets (e.g. {@code -04:00}) for the survey/task tables — those come not from
 * {@code clientTimeZone} but from the offset embedded in the assessment payload's ISO timestamps (info.json
 * {@code createdOn}, gonogo/trailmaking {@code startDate}). Keyboard sessions use epoch timestamps with no offset, so
 * their {@code time_zone} is left empty — matching the golden {@code keyboard_sessions} preview. This class therefore
 * <em>extracts</em> the offset from a payload timestamp rather than fabricating {@code Z}. Flagged for review: if the
 * FAIR workbook mandates {@code Z}-only, switch {@link #offsetFromIso} callers to emit null/Z.</p>
 */
public final class AddfDateUtils {
    private static final DateTimeFormatter UTC_FORMAT = ISODateTimeFormat.dateTime().withZoneUTC();
    // Parser that preserves the offset present in the input string.
    private static final DateTimeFormatter OFFSET_PARSER = ISODateTimeFormat.dateTimeParser().withOffsetParsed();

    private AddfDateUtils() {
    }

    /**
     * Convert Unix epoch <b>seconds</b> (as the keyboard {@code Session.json} keylog {@code timestamp}s use) to
     * ISO-8601 UTC. Null-safe. There is no capture-time offset in these values, so keyboard rows have a null
     * {@code time_zone} (matching the golden {@code keyboard_sessions} preview).
     */
    public static String epochSecondsToUtcIso(Double epochSeconds) {
        if (epochSeconds == null) {
            return null;
        }
        // Truncate the sub-millisecond remainder rather than rounding it: the delivered data does, and rounding shifts
        // a timestamp like 1787223907.229764 forward to ...230Z where the golden has ...229Z. One millisecond is
        // immaterial to the science but a silent off-by-one against the contract is not.
        return UTC_FORMAT.print(new DateTime((long) (epochSeconds * 1000d), DateTimeZone.UTC));
    }

    /** Format a joda {@link DateTime} as ISO-8601 UTC ({@code …Z}). Null-safe. */
    public static String toUtcIso(DateTime dateTime) {
        if (dateTime == null) {
            return null;
        }
        return UTC_FORMAT.print(dateTime.withZone(DateTimeZone.UTC));
    }

    /** Today's UTC calendar date ({@code YYYY-MM-DD}) — the accumulate/stage-date partition. */
    public static String todayUtcDate() {
        return new LocalDate(DateTimeZone.UTC).toString();
    }

    /** The UTC calendar date ({@code YYYY-MM-DD}) of a joda {@link DateTime}; null-safe. Used for the raw/ partition. */
    public static String utcDate(DateTime dateTime) {
        if (dateTime == null) {
            return null;
        }
        return dateTime.withZone(DateTimeZone.UTC).toLocalDate().toString();
    }

    /** Parse an ISO string and re-emit it as ISO-8601 UTC. Null-safe; returns null on unparseable input. */
    public static String toUtcIso(String isoString) {
        if (isoString == null || isoString.isEmpty()) {
            return null;
        }
        try {
            return UTC_FORMAT.print(OFFSET_PARSER.parseDateTime(isoString).withZone(DateTimeZone.UTC));
        } catch (RuntimeException ex) {
            return null;
        }
    }

    /**
     * Epoch millis of an ISO timestamp, or null when the input is null/empty/unparseable. Used to order two candidate
     * timestamps by instant rather than by string — fractional-second digits vary between payloads, so lexicographic
     * comparison of two ISO strings is not reliably chronological.
     */
    public static Long epochMillis(String isoString) {
        if (isoString == null || isoString.isEmpty()) {
            return null;
        }
        try {
            return OFFSET_PARSER.parseDateTime(isoString).getMillis();
        } catch (RuntimeException ex) {
            return null;
        }
    }

    /**
     * Recover the UTC offset (e.g. {@code -04:00}, {@code +00:00}) from an offset-bearing ISO timestamp, for the
     * {@code time_zone} column. Returns null when the input is null, unparseable, or has no explicit offset.
     */
    public static String offsetFromIso(String isoString) {
        if (isoString == null || isoString.isEmpty()) {
            return null;
        }
        try {
            DateTime parsed = OFFSET_PARSER.parseDateTime(isoString);
            int offsetMillis = parsed.getZone().getOffset(parsed.getMillis());
            return formatOffset(offsetMillis);
        } catch (RuntimeException ex) {
            return null;
        }
    }

    private static String formatOffset(int offsetMillis) {
        String sign = offsetMillis < 0 ? "-" : "+";
        int abs = Math.abs(offsetMillis);
        int minutes = abs / 60000;
        int hours = minutes / 60;
        int mins = minutes % 60;
        return String.format("%s%02d:%02d", sign, hours, mins);
    }
}
