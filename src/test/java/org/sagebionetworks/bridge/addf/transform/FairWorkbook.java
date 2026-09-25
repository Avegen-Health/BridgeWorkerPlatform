package org.sagebionetworks.bridge.addf.transform;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * A tiny, dependency-free reader for the FAIR metadata workbook — just enough of the xlsx format to pull one sheet out
 * by <em>name</em> and hand back its cells keyed by column letter.
 *
 * <p><b>Why not Apache POI.</b> This exists only to let {@code AddfTablesFairConformanceTest} read a 156-row sheet.
 * Pulling poi-ooxml in would add xmlbeans, commons-compress and a second log4j binding to a WAR whose Parquet/Hadoop
 * dependency tree was already deliberately trimmed (§3.9) — a real conflict risk for one test. The xlsx container is a
 * zip of XML, and the JDK reads both.</p>
 *
 * <p>Two details that matter and are easy to get wrong:</p>
 * <ul>
 *   <li><b>Resolve the sheet by name, not by file number.</b> {@code xl/worksheets/sheetN.xml} has no stable relation
 *       to tab order; the mapping runs sheet name → {@code r:id} → relationship target. Reading {@code sheet8.xml}
 *       because "fields is the 8th tab" silently reads a different sheet the moment someone reorders tabs.</li>
 *   <li><b>Key cells by column letter, not by position.</b> Excel omits empty cells entirely, so a row with a blank
 *       {@code constraints} column has its {@code description} land in slot 5. Positional reading shifts values into
 *       the wrong columns on exactly the rows a conformance test cares about.</li>
 * </ul>
 */
final class FairWorkbook {
    private final Map<String, byte[]> entries = new HashMap<>();
    private final List<String> sharedStrings = new ArrayList<>();

    private FairWorkbook() {
    }

    /** Read the whole workbook into memory (it is tens of KB) and index its parts by zip entry name. */
    static FairWorkbook read(InputStream in) throws IOException, SAXException, ParserConfigurationException {
        FairWorkbook workbook = new FairWorkbook();
        try (ZipInputStream zip = new ZipInputStream(in)) {
            ZipEntry entry;
            while ((entry = zip.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    workbook.entries.put(entry.getName(), drain(zip));
                }
            }
        }
        workbook.loadSharedStrings();
        return workbook;
    }

    /**
     * Every row of the named sheet, as column-letter → cell-text maps. The header row is included; callers skip it.
     * Rows whose cells are all empty are dropped, so a trailing block of blank rows doesn't look like missing fields.
     */
    List<Map<String, String>> rows(String sheetName) throws IOException, SAXException, ParserConfigurationException {
        byte[] sheetXml = entries.get(resolveSheetPath(sheetName));
        if (sheetXml == null) {
            throw new IllegalArgumentException("No sheet named '" + sheetName + "' in the workbook");
        }
        Element sheetData = firstChild(parse(sheetXml).getDocumentElement(), "sheetData");
        List<Map<String, String>> rows = new ArrayList<>();
        for (Element row : childrenNamed(sheetData, "row")) {
            Map<String, String> cells = new LinkedHashMap<>();
            for (Element cell : childrenNamed(row, "c")) {
                String value = cellText(cell);
                if (!value.isEmpty()) {
                    cells.put(columnLetter(cell.getAttribute("r")), value);
                }
            }
            if (!cells.isEmpty()) {
                rows.add(cells);
            }
        }
        return rows;
    }

    // -----------------------------------------------------------------------------------------------------------
    // xlsx plumbing
    // -----------------------------------------------------------------------------------------------------------

    /** sheet name → r:id (workbook.xml) → relationship target (workbook.xml.rels) → zip entry name. */
    private String resolveSheetPath(String sheetName) throws IOException, SAXException, ParserConfigurationException {
        Document workbook = parse(entries.get("xl/workbook.xml"));
        String relationshipId = null;
        NodeList sheets = workbook.getElementsByTagName("sheet");
        for (int i = 0; i < sheets.getLength(); i++) {
            Element sheet = (Element) sheets.item(i);
            if (sheetName.equals(sheet.getAttribute("name"))) {
                relationshipId = sheet.getAttribute("r:id");
                break;
            }
        }
        if (relationshipId == null) {
            throw new IllegalArgumentException("No sheet named '" + sheetName + "' in the workbook");
        }

        Document rels = parse(entries.get("xl/_rels/workbook.xml.rels"));
        NodeList relationships = rels.getElementsByTagName("Relationship");
        for (int i = 0; i < relationships.getLength(); i++) {
            Element relationship = (Element) relationships.item(i);
            if (relationshipId.equals(relationship.getAttribute("Id"))) {
                String target = relationship.getAttribute("Target");
                // Targets are either absolute in the package ("/xl/worksheets/sheet8.xml") or relative to xl/.
                return target.startsWith("/") ? target.substring(1) : "xl/" + target;
            }
        }
        throw new IllegalArgumentException("Sheet '" + sheetName + "' has no relationship " + relationshipId);
    }

    /** Older/re-saved workbooks intern their strings; this one uses inline strings, so the part may be absent. */
    private void loadSharedStrings() throws IOException, SAXException, ParserConfigurationException {
        byte[] xml = entries.get("xl/sharedStrings.xml");
        if (xml == null) {
            return;
        }
        for (Element si : childrenNamed(parse(xml).getDocumentElement(), "si")) {
            sharedStrings.add(concatText(si));
        }
    }

    private String cellText(Element cell) {
        String type = cell.getAttribute("t");
        if ("inlineStr".equals(type)) {
            Element is = firstChild(cell, "is");
            return is == null ? "" : concatText(is);
        }
        Element v = firstChild(cell, "v");
        String raw = v == null ? "" : text(v);
        if ("s".equals(type)) {
            int index = Integer.parseInt(raw.trim());
            return index < sharedStrings.size() ? sharedStrings.get(index) : "";
        }
        return raw;
    }

    /** A cell reference is column letters followed by a row number ({@code D12} → {@code D}). */
    private static String columnLetter(String cellReference) {
        int end = 0;
        while (end < cellReference.length() && Character.isLetter(cellReference.charAt(end))) {
            end++;
        }
        return cellReference.substring(0, end);
    }

    /** Rich text splits a cell across several {@code <t>} runs; the cell's value is their concatenation. */
    private static String concatText(Element element) {
        StringBuilder sb = new StringBuilder();
        NodeList tags = element.getElementsByTagName("t");
        for (int i = 0; i < tags.getLength(); i++) {
            sb.append(text((Element) tags.item(i)));
        }
        return sb.toString();
    }

    private static String text(Element element) {
        String value = element.getTextContent();
        return value == null ? "" : value;
    }

    private static Element firstChild(Element parent, String name) {
        for (Element child : childrenNamed(parent, name)) {
            return child;
        }
        return null;
    }

    private static List<Element> childrenNamed(Element parent, String name) {
        List<Element> matches = new ArrayList<>();
        if (parent == null) {
            return matches;
        }
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() == Node.ELEMENT_NODE && name.equals(child.getNodeName())) {
                matches.add((Element) child);
            }
        }
        return matches;
    }

    private static Document parse(byte[] xml) throws IOException, SAXException, ParserConfigurationException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        // The workbook is a build artefact we ship, but an XML parser with entity resolution left on is a habit worth
        // not forming.
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setExpandEntityReferences(false);
        DocumentBuilder builder = factory.newDocumentBuilder();
        return builder.parse(new ByteArrayInputStream(xml));
    }

    private static byte[] drain(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int read;
        while ((read = in.read(buffer)) != -1) {
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }
}
