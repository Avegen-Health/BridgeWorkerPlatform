package org.sagebionetworks.bridge.addf.transform;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.addf.decrypt.DecryptedArchive;
import org.sagebionetworks.bridge.rest.model.HealthDataRecordEx3;

public class RecordFlattenerTest {
    private RecordFlattener flattener;
    private ActivityRowBuilder mockBuilder;
    private DemographicsBuilder mockDemographics;

    @BeforeMethod
    public void before() {
        mockBuilder = mock(ActivityRowBuilder.class);
        when(mockBuilder.handledItems()).thenReturn(new String[] { "MyItem" });
        mockDemographics = mock(DemographicsBuilder.class);

        flattener = new RecordFlattener();
        flattener.setActivityRowBuilders(ImmutableList.of(mockBuilder));
        flattener.setDemographicsBuilder(mockDemographics);
    }

    private static DecryptedArchive archive(Map<String, JsonNode> json, Map<String, File> unzipped,
            Map<String, String> metadata) {
        HealthDataRecordEx3 record = mock(HealthDataRecordEx3.class);
        when(record.getMetadata()).thenReturn(metadata);
        return new DecryptedArchive(record, null, null, unzipped, json);
    }

    @Test
    public void resolveItemFromInfoJson() {
        DecryptedArchive archive = archive(
                ImmutableMap.of("info.json", AddfTestFixtures.node("{\"item\":\"PHQ-9\"}")),
                Collections.<String, File>emptyMap(), null);
        assertEquals(flattener.resolveItem(archive), "PHQ-9");
    }

    @Test
    public void resolveItemFromMetadataItem() {
        DecryptedArchive archive = archive(Collections.<String, JsonNode>emptyMap(),
                Collections.<String, File>emptyMap(), ImmutableMap.of("item", "daily"));
        assertEquals(flattener.resolveItem(archive), "daily");
    }

    @Test
    public void resolveItemFromMetadataAssessmentId() {
        DecryptedArchive archive = archive(Collections.<String, JsonNode>emptyMap(),
                Collections.<String, File>emptyMap(), ImmutableMap.of("assessmentId", "Evening_Log"));
        assertEquals(flattener.resolveItem(archive), "Evening_Log");
    }

    @Test
    public void resolveItemInfersKeyboardFromSessionFile() {
        Map<String, File> unzipped = new HashMap<>();
        unzipped.put("Session.json", new File("Session.json"));
        DecryptedArchive archive = archive(Collections.<String, JsonNode>emptyMap(), unzipped, null);
        assertEquals(flattener.resolveItem(archive), "KeyboardSession");
    }

    @Test
    public void resolveItemInfersGoNoGoAndTrailMaking() {
        Map<String, File> gonogo = new HashMap<>();
        gonogo.put("gonogo.json", new File("gonogo.json"));
        assertEquals(flattener.resolveItem(archive(Collections.<String, JsonNode>emptyMap(), gonogo, null)),
                "gonogo");

        Map<String, File> trail = new HashMap<>();
        trail.put("trailmaking.json", new File("trailmaking.json"));
        assertEquals(flattener.resolveItem(archive(Collections.<String, JsonNode>emptyMap(), trail, null)),
                "trailmaking");
    }

    @Test
    public void resolveItemNullWhenUndeterminable() {
        DecryptedArchive archive = archive(Collections.<String, JsonNode>emptyMap(),
                Collections.<String, File>emptyMap(), null);
        assertNull(flattener.resolveItem(archive));
    }

    @Test
    public void flattenContentNullItem() {
        FlattenContext ctx = AddfTestFixtures.context(new HashMap<String, JsonNode>());
        assertNull(flattener.flattenContent(ctx, null));
    }

    @Test
    public void flattenContentDispatchesToDemographics() {
        FlattenContext ctx = AddfTestFixtures.context(new HashMap<String, JsonNode>());
        TableRow demoRow = new TableRow("demographics", "hc");
        when(mockDemographics.handles("birth-gender")).thenReturn(true);
        when(mockDemographics.build(ctx, "birth-gender")).thenReturn(demoRow);
        assertSame(flattener.flattenContent(ctx, "birth-gender"), demoRow);
    }

    @Test
    public void flattenContentDispatchesToActivityBuilderCaseInsensitively() {
        FlattenContext ctx = AddfTestFixtures.context(new HashMap<String, JsonNode>());
        TableRow row = new TableRow("t", "k");
        when(mockDemographics.handles(any(String.class))).thenReturn(false);
        when(mockBuilder.build(eq(ctx))).thenReturn(row);
        assertSame(flattener.flattenContent(ctx, "myITEM"), row);
    }

    @Test
    public void flattenContentUnmappedItemReturnsNull() {
        FlattenContext ctx = AddfTestFixtures.context(new HashMap<String, JsonNode>());
        when(mockDemographics.handles(any(String.class))).thenReturn(false);
        assertNull(flattener.flattenContent(ctx, "no-such-item"));
    }
}
