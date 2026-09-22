package org.sagebionetworks.bridge.addf.decrypt;

import java.io.File;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import org.sagebionetworks.bridge.rest.model.HealthDataRecordEx3;
import org.sagebionetworks.bridge.rest.model.Upload;

/**
 * The in-memory/temp-dir result of fetching, decrypting, and unzipping one upload archive for ADDF. Produced by
 * {@link UploadFetcher}; consumed by the transform stack.
 *
 * <p>Holds the parsed JSON payload files ({@code Session.json}, {@code answers.json}, {@code gonogo.json},
 * {@code trailmaking.json}, {@code metadata.json}, {@code info.json}, …) keyed by filename, the raw decrypted archive
 * ({@code File}) to copy verbatim into the export store's {@code raw/} layer, and the source {@link HealthDataRecordEx3}
 * / {@link Upload}. The backing temp directory is owned by the caller and cleaned in its {@code finally}.</p>
 */
public class DecryptedArchive {
    private final HealthDataRecordEx3 record;
    private final Upload upload;
    private final File decryptedArchiveFile;
    private final Map<String, File> unzippedFiles;
    private final Map<String, JsonNode> jsonFiles;

    public DecryptedArchive(HealthDataRecordEx3 record, Upload upload, File decryptedArchiveFile,
            Map<String, File> unzippedFiles, Map<String, JsonNode> jsonFiles) {
        this.record = record;
        this.upload = upload;
        this.decryptedArchiveFile = decryptedArchiveFile;
        this.unzippedFiles = unzippedFiles;
        this.jsonFiles = jsonFiles;
    }

    public HealthDataRecordEx3 getRecord() {
        return record;
    }

    public Upload getUpload() {
        return upload;
    }

    /** The decrypted archive (a zip of the assessment's JSON files), for verbatim copy into {@code raw/}. */
    public File getDecryptedArchiveFile() {
        return decryptedArchiveFile;
    }

    public Map<String, File> getUnzippedFiles() {
        return unzippedFiles;
    }

    /** Parsed JSON payload files by filename; a filename absent here either wasn't present or wasn't valid JSON. */
    public JsonNode getJson(String filename) {
        return jsonFiles.get(filename);
    }

    public boolean hasFile(String filename) {
        return unzippedFiles.containsKey(filename);
    }
}
