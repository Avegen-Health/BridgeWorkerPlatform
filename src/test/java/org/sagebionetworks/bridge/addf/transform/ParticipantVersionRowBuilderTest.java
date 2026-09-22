package org.sagebionetworks.bridge.addf.transform;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.rest.model.SharingScope;

public class ParticipantVersionRowBuilderTest {
    private static final String HEALTH_CODE = "health-code";
    private static final DateTime CREATED_ON = new DateTime(2022, 1, 14, 1, 19, 21, 201, DateTimeZone.forOffsetHours(-8));
    private static final DateTime MODIFIED_ON = new DateTime(2022, 1, 15, 2, 0, 0, 0, DateTimeZone.UTC);

    private ParticipantVersionRowBuilder builder;

    @BeforeMethod
    public void before() {
        builder = new ParticipantVersionRowBuilder();
    }

    private static ParticipantVersion baseVersion() {
        ParticipantVersion pv = mock(ParticipantVersion.class);
        when(pv.getHealthCode()).thenReturn(HEALTH_CODE);
        when(pv.getParticipantVersion()).thenReturn(3);
        when(pv.getCreatedOn()).thenReturn(CREATED_ON);
        when(pv.getModifiedOn()).thenReturn(MODIFIED_ON);
        return pv;
    }

    @Test
    public void singleStudyMembership() {
        ParticipantVersion pv = baseVersion();
        when(pv.getStudyMemberships()).thenReturn(ImmutableMap.of("biaffect-3-study", "ext-1"));
        when(pv.getSharingScope()).thenReturn(SharingScope.SPONSORS_AND_PARTNERS);
        when(pv.getDataGroups()).thenReturn(ImmutableList.of("bbb", "aaa"));
        when(pv.getLanguages()).thenReturn(ImmutableList.of("en", "es"));
        when(pv.getClientTimeZone()).thenReturn("America/New_York");

        TableRow row = builder.build(pv, false);

        assertEquals(row.getTable(), AddfTables.PARTICIPANT_VERSIONS);
        assertEquals(row.getKey(), HEALTH_CODE);
        assertEquals(row.get("health_code"), HEALTH_CODE);
        assertEquals(row.get("participant_version"), 3);
        assertEquals(row.get("study_id"), "biaffect-3-study");
        assertEquals(row.get("external_id"), "ext-1");
        assertEquals(row.get("study_memberships"), "|biaffect-3-study=ext-1|");
        assertEquals(row.get("sharing_scope"), SharingScope.SPONSORS_AND_PARTNERS.toString());
        assertEquals(row.get("data_groups"), "aaa,bbb");
        assertEquals(row.get("languages"), "en|es");
        assertEquals(row.get("client_time_zone"), "America/New_York");
        assertEquals(row.get("created_on"), "2022-01-14T09:19:21.201Z");
        assertEquals(row.get("modified_on"), "2022-01-15T02:00:00.000Z");
    }

    @Test
    public void nullMembershipsAndCollections() {
        ParticipantVersion pv = baseVersion();
        when(pv.getStudyMemberships()).thenReturn(null);
        when(pv.getSharingScope()).thenReturn(null);
        when(pv.getDataGroups()).thenReturn(null);
        when(pv.getLanguages()).thenReturn(null);
        when(pv.getClientTimeZone()).thenReturn(null);

        TableRow row = builder.build(pv, false);

        assertNull(row.get("study_id"));
        assertNull(row.get("external_id"));
        assertNull(row.get("study_memberships"));
        assertNull(row.get("sharing_scope"));
        assertNull(row.get("data_groups"));
        assertNull(row.get("languages"));
    }

    @Test
    public void multiStudyMembershipSortsKeysAndNullsExternalId() {
        ParticipantVersion pv = baseVersion();
        when(pv.getStudyMemberships()).thenReturn(ImmutableMap.of("s2", "x", "s1", "y"));

        TableRow row = builder.build(pv, false);

        assertEquals(row.get("study_id"), "s1,s2");
        assertNull(row.get("external_id"));
        assertEquals(row.get("study_memberships"), "|s1=y|s2=x|");
    }

    @Test
    public void extIdNoneSentinelBecomesNullAndBlankSerialisation() {
        ParticipantVersion pv = baseVersion();
        when(pv.getStudyMemberships()).thenReturn(
                ImmutableMap.of("only-study", ParticipantVersionRowBuilder.EXT_ID_NONE));

        TableRow row = builder.build(pv, false);

        assertEquals(row.get("study_id"), "only-study");
        assertNull(row.get("external_id"));
        assertEquals(row.get("study_memberships"), "|only-study=|");
    }
}
