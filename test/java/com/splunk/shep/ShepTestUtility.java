// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.splunk.shep;

import static com.splunk.shep.ShepConstants.*;
import static com.splunk.shep.ShepConstants.OutputMode.*;
import static com.splunk.shep.exporter.SplunkEventFormatter.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.splunk.Args;
import com.splunk.EntityCollection;
import com.splunk.Index;
import com.splunk.Service;
import com.splunk.shep.ShepConstants.OutputMode;
import com.splunk.shep.exporter.model.ShepExport;
import com.splunk.shep.exporter.model.SplunkEvent;
import com.splunk.shep.mapreduce.lib.rest.SplunkXMLStream;

/**
 * @author hyan
 *
 */
public class ShepTestUtility {
    private static final Logger log = Logger.getLogger(ShepTestUtility.class);
    private static final String XML_TAG = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>";

    public static final String TEMP_DIR_PATH = FileUtils.getTempDirectoryPath();
    public static final String BASE_DIR_PATH = System.getProperty("user.dir");
    // splunk convert your index name to all lowercase, so use lowercase to make
    // sure you can find the index
    public static final String TEST_INDEX_NAME = "shepTestIndex".toLowerCase();
    public static final String[] SHEP_EXPORT_EVENT_FIELDS = {
	    SPLUNK_EVENT_FIELD_RAW, SPLUNK_EVENT_FIELD_SOURCE,
	    SPLUNK_EVENT_FIELD_SOURCETYPE, SPLUNK_EVENT_FIELD_TIME };
    public static final SimpleDateFormat SPLUNK_DATE_FORMAT = new SimpleDateFormat(
	    "yyyy-MM-dd HH:mm:ss.000 z");

    // SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm.000 z");

    public static Service createService() throws ConfigurationException {
	Configuration conf = new PropertiesConfiguration(
		SHEP_DEFAULT_PROPERTIES_FILE_NAME);
	Args args = new Args();
	args.put("username", conf.getString(SPLUNK_USER_NAME));
	args.put("password", conf.getString(SPLUNK_PASSWORD));
	args.put("host", conf.getString(SPLUNK_HOST));
	args.put("port", conf.getInt(SPLUNK_MGMT_PORT));

	return Service.connect(args);
    }

    public static void initTestEnv() {
	// if you run test from ant, it will set SPLUNK_HOME_PROPERTY first. So
	// these setting are not required.
	// This is in case you already start a splunk and want to use that
	// splunk for testing
	File file = FileUtils.getFile(BASE_DIR_PATH, "build-cache", "splunk");
	if (System.getProperty(SPLUNK_HOME_PROPERTY) == null) {
	    System.setProperty(SPLUNK_HOME_PROPERTY, file.getAbsolutePath());
	}
	File shepHome = new File(SHEP_HOME);
	if (!shepHome.exists()) {
	    shepHome.mkdirs();
	}
    }

    public static void deleteTranslog() {
	File translog = new File(TRANSLOG_FILE_PATH);
	if (translog.exists()) {
	    translog.delete();
	}
    }

    public static void waitEventCount(Index index, int value, int seconds) {
	int oriSeconds = seconds;
	while (seconds > 0) {
	    sleep(1000);
	    seconds = seconds - 1;
	    if (index.getTotalEventCount() == value) {
		break;
	    }
	    index.refresh();
	}
	log.debug(String.format("waitEventCount for index %s took %s sec",
		index.getName(), (oriSeconds - seconds)));
    }

    public static void addOneShot(Service service, String indexName,
	    ShepExport shepExport) throws IOException {
	List<SplunkEvent> splunkEvents = shepExport.getSplunkEvent();
	List<String> events = new ArrayList<String>();
	for (SplunkEvent event : splunkEvents) {
	    events.add(event.getRaw());
	}
	addOneShot(service, indexName, events.toArray(new String[0]));
    }

    public static void addOneShot(Service service, String indexName,
	    String... lines) throws IOException {
	long st = System.currentTimeMillis();
	EntityCollection<Index> indexes = service.getIndexes();
	indexName = indexName.toLowerCase();
	if (!indexes.containsKey(indexName)) {
	    indexes.create(indexName);
	    indexes.refresh();
	}

	assertTrue(indexes.containsKey(indexName));
	Index index = indexes.get(indexName);
	index.clean(60);
	assertEquals(index.getTotalEventCount(), 0);
	for (String line : lines) {
	    index.submit(line);
	}

	waitEventCount(index, lines.length, 60);
	log.debug(String.format("addOneShot to index %s took %d sec",
		indexName, (System.currentTimeMillis() - st) / 1000));
	assertEquals(index.getTotalEventCount(), lines.length);
    }

    public static String[] prefixTime(String... lines) {
	String[] result = new String[lines.length];
	for (int i = 0; i < lines.length; i++) {
	    result[i] = prefixTime(lines[i]);
	}
	return result;
    }

    public static String prefixTime(String line) {
	String date = SPLUNK_DATE_FORMAT.format(new Date());
	return String.format("%s %s", date, line);
    }

    public static void verifyFields(String content, ShepExport shepExport,
	    OutputMode mode) throws Exception {
	List<Map<String, String>> fieldMaps = new ArrayList<Map<String, String>>();
	List<SplunkEvent> splunkEvents = shepExport.getSplunkEvent();
	for (int i = splunkEvents.size() - 1; i >= 0; i--) {
	    SplunkEvent event = splunkEvents.get(i);
	    Map<String, String> fieldMap = getFieldMap(event);
	    fieldMaps.add(fieldMap);
	}

	if (mode == json) {
	    verifyJsonFields(content, fieldMaps);
	} else if (mode == xml) {
	    verifyXmlFields(content, fieldMaps);
	}
    }

    private static void verifyJsonFields(String content,
	    List<Map<String, String>> fieldMaps) throws Exception {
	ObjectMapper mapper = new ObjectMapper();
	JsonNode root = mapper.readTree(content);
	assertNotNull(root);
	for (int i = 0; i < fieldMaps.size(); i++) {
	    Map<String, String> fieldMap = fieldMaps.get(i);
	    JsonNode node = root.get(i);
	    for (Entry<String, String> entry : fieldMap.entrySet()) {
		assertEquals(node.get(entry.getKey()).getTextValue(),
			entry.getValue());
	    }
	}
    }

    private static void verifyXmlFields(String content,
	    List<Map<String, String>> fieldMaps) throws Exception {
	SplunkXMLStream parser = new SplunkXMLStream(
		IOUtils.toInputStream(content));
	HashMap<String, String> actualFieldMap = null;
	int i = 0;
	while (true) {
	    actualFieldMap = parser.nextResult();
	    if (actualFieldMap == null) {
		break;
	    }
	    for (Entry<String, String> entry : fieldMaps.get(i).entrySet()) {
		assertEquals(actualFieldMap.get(entry.getKey()),
			entry.getValue());
	    }
	    i++;
	}
    }

    public static <T> void verifyFields(String content, Class<T> clazz,
	    Map<String, String> fieldMap, boolean normalize, OutputMode mode)
	    throws Exception {
	T target = getObject(content, clazz, mode);
	verifyObject(target, clazz, fieldMap, normalize);
    }

    public static <T> void verifyFieldsNotNull(String content, Class<T> clazz,
	    boolean normalize, OutputMode mode, String... fieldNames)
	    throws Exception {
	T target = getObject(content, clazz, mode);
	verifyNotNull(target, clazz, normalize, fieldNames);
    }

    public static String readFile(File file) throws IOException {
	return IOUtils.toString(new FileReader(file));
    }

    static <T> void verifyObject(T target, Class<T> clazz,
	    Map<String, String> fieldMap, boolean normalize)
	    throws Exception {
	for (Entry<String, String> entry : fieldMap.entrySet()) {
	    assertEquals(FieldUtils.readField(target,
		    normalize ? normalizeFieldName(entry.getKey())
			    : entry.getKey(), true),
		    entry.getValue());
	}
    }
    
    static <T> T getObject(String content, Class<T> clazz, OutputMode mode)
	    throws Exception {
	T target = null;
	if (mode == xml) {
	    target = xmlToObject(content, clazz);
	} else if (mode == json) {
	    target = jsonToObject(content, clazz);
	} else if (mode == csv) {

	}
	return target;
    }

    static <T> void verifyNotNull(T target, Class<T> clazz,
	    boolean normalizeFieldName, String... fieldNames) throws Exception {
	assertNotNull(target);
	for (String fieldName : fieldNames) {
	    assertNotNull(FieldUtils.readField(target,
		    normalizeFieldName ? normalizeFieldName(fieldName)
			    : fieldName, true));
	}
    }

    static String normalizeFieldName(String fieldName) {
	fieldName = fieldName.replaceAll("_", "");
	return fieldName;
    }

    public static SplunkEvent getSplunkEvent(String content) {
	String time = SPLUNK_DATE_FORMAT.format(new Date());
	log.debug("time: " + time);

	String raw = time + " " + content;
	String source = "http-simple";
	String sourcetype = "unknown-too_small";

	SplunkEvent splunkEvent = new SplunkEvent();
	splunkEvent.setRaw(raw);
	splunkEvent.setSource(source);
	splunkEvent.setSourcetype(sourcetype);
	splunkEvent.setTime(time);
	return splunkEvent;
    }

    public static ShepExport getShepExport(String content, int numEvents) {
	ShepExport shepExport = new ShepExport();
	for (int i = 1; i <= numEvents; i++) {
	    shepExport.getSplunkEvent().add(getSplunkEvent(content + " " + i));
	}
	return shepExport;
    }

    public static Map<String, String> getFieldMap(SplunkEvent splunkEvent) {
	Map<String, String> fieldMap = new HashMap<String, String>();
	fieldMap.put(SPLUNK_EVENT_FIELD_RAW, splunkEvent.getRaw());
	fieldMap.put(SPLUNK_EVENT_FIELD_SOURCE, splunkEvent.getSource());
	fieldMap.put(SPLUNK_EVENT_FIELD_SOURCETYPE, splunkEvent.getSourcetype());
	fieldMap.put(SPLUNK_EVENT_FIELD_TIME, splunkEvent.getTime());
	log.debug("fieldMap: " + fieldMap);
	return fieldMap;
    }

    public static String getExpectedSplunkEventXml(SplunkEvent splunkEvent,
	    boolean prefixXmlTag) {
	String expectedXml = String
		.format("<splunkEvent><%s>%s</%s><%s>%s</%s><%s>%s</%s><%s>%s</%s></splunkEvent>",
			SPLUNK_EVENT_FIELD_RAW, splunkEvent.getRaw(),
			SPLUNK_EVENT_FIELD_RAW, SPLUNK_EVENT_FIELD_SOURCE,
			splunkEvent.getSource(), SPLUNK_EVENT_FIELD_SOURCE,
			SPLUNK_EVENT_FIELD_SOURCETYPE,
			splunkEvent.getSourcetype(),
			SPLUNK_EVENT_FIELD_SOURCETYPE, SPLUNK_EVENT_FIELD_TIME,
			splunkEvent.getTime(), SPLUNK_EVENT_FIELD_TIME);
	if (prefixXmlTag) {
	    expectedXml = XML_TAG + expectedXml;
	}
	log.debug("expectedSplunkEventXml: " + expectedXml);
	return expectedXml;
    }

    public static String getExpectedShepExportXml(ShepExport shepExport) {
	List<SplunkEvent> splunkEvents = shepExport.getSplunkEvent();
	StringBuilder sb = new StringBuilder();
	sb.append(XML_TAG).append("<shepExport>");
	for (SplunkEvent splunkEvent : splunkEvents) {
	    sb.append(getExpectedSplunkEventXml(splunkEvent, false));
	}
	sb.append("</shepExport>");
	log.debug("expectedShepExportXml: " + sb.toString());
	return sb.toString();
    }

    public static String getExpectedSplunkEventJson(SplunkEvent splunkEvent) {
	String expectedJson = String.format(
		"{\"%s\":\"%s\",\"%s\":\"%s\",\"%s\":\"%s\",\"%s\":\"%s\"}",
		SPLUNK_EVENT_FIELD_RAW, splunkEvent.getRaw(),
		SPLUNK_EVENT_FIELD_SOURCE, splunkEvent.getSource(),
		SPLUNK_EVENT_FIELD_SOURCETYPE, splunkEvent.getSourcetype(),
		SPLUNK_EVENT_FIELD_TIME, splunkEvent.getTime());
	log.debug("expectedSplunkEventJson: " + expectedJson);
	return expectedJson;
    }

    public static String getExpectedShepExportJson(ShepExport shepExport) {
	List<SplunkEvent> splunkEvents = shepExport.getSplunkEvent();
	StringBuilder sb = new StringBuilder();
	sb.append("[");
	for (SplunkEvent splunkEvent : splunkEvents) {
	    sb.append(getExpectedSplunkEventJson(splunkEvent));
	    sb.append(",");
	}
	sb.deleteCharAt(sb.length() - 1);
	sb.append("]");
	log.debug("expectedShepExportJson: " + sb.toString());
	return sb.toString();
    }

    public static void sleep(long millis) {
	try {
	    Thread.sleep(millis);
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}
    }

}
