package org.sagebionetworks.bridge.addf.transform;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

public class AddfDateUtilsTest {
    @Test
    public void epochSecondsToUtcIso_null() {
        assertNull(AddfDateUtils.epochSecondsToUtcIso(null));
    }

    @Test
    public void epochSecondsToUtcIso_value() {
        // 2021-01-01T00:00:00Z == 1609459200 seconds.
        assertEquals(AddfDateUtils.epochSecondsToUtcIso(1609459200d), "2021-01-01T00:00:00.000Z");
    }

    @Test
    public void toUtcIso_dateTime_null() {
        assertNull(AddfDateUtils.toUtcIso((DateTime) null));
    }

    @Test
    public void toUtcIso_dateTime_convertsToUtc() {
        DateTime dt = new DateTime(2022, 1, 14, 1, 19, 21, 201, DateTimeZone.forOffsetHours(-8));
        assertEquals(AddfDateUtils.toUtcIso(dt), "2022-01-14T09:19:21.201Z");
    }

    @Test
    public void utcDate_null() {
        assertNull(AddfDateUtils.utcDate(null));
    }

    @Test
    public void utcDate_value() {
        DateTime dt = new DateTime(2022, 1, 14, 1, 19, 21, DateTimeZone.forOffsetHours(-8));
        // 01:19 -08:00 is 09:19 UTC, same calendar day.
        assertEquals(AddfDateUtils.utcDate(dt), "2022-01-14");
    }

    @Test
    public void todayUtcDate_isTenCharIsoDate() {
        String today = AddfDateUtils.todayUtcDate();
        assertEquals(today.length(), 10);
        assertEquals(today.charAt(4), '-');
        assertEquals(today.charAt(7), '-');
    }

    @Test
    public void toUtcIso_string_null() {
        assertNull(AddfDateUtils.toUtcIso((String) null));
    }

    @Test
    public void toUtcIso_string_empty() {
        assertNull(AddfDateUtils.toUtcIso(""));
    }

    @Test
    public void toUtcIso_string_unparseable() {
        assertNull(AddfDateUtils.toUtcIso("not-a-date"));
    }

    @Test
    public void toUtcIso_string_withOffset() {
        assertEquals(AddfDateUtils.toUtcIso("2026-08-15T10:30:00.000-04:00"), "2026-08-15T14:30:00.000Z");
    }

    @Test
    public void offsetFromIso_null() {
        assertNull(AddfDateUtils.offsetFromIso(null));
    }

    @Test
    public void offsetFromIso_empty() {
        assertNull(AddfDateUtils.offsetFromIso(""));
    }

    @Test
    public void offsetFromIso_unparseable() {
        assertNull(AddfDateUtils.offsetFromIso("garbage"));
    }

    @Test
    public void offsetFromIso_negativeOffset() {
        assertEquals(AddfDateUtils.offsetFromIso("2026-08-15T10:30:00.000-04:00"), "-04:00");
    }

    @Test
    public void offsetFromIso_zuluIsPlusZero() {
        assertEquals(AddfDateUtils.offsetFromIso("2026-08-15T10:30:00.000Z"), "+00:00");
    }

    @Test
    public void offsetFromIso_positiveHalfHourOffset() {
        assertEquals(AddfDateUtils.offsetFromIso("2026-08-15T10:30:00.000+05:30"), "+05:30");
    }
}
