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
package com.splunk.shep.exporter;

import static com.splunk.shep.ShepConstants.*;
import static com.splunk.shep.ShepConstants.OutputMode.*;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.AnnotationIntrospector;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.xc.JaxbAnnotationIntrospector;

import com.splunk.shep.ShepConstants.OutputMode;
import com.splunk.shep.exporter.model.ShepExport;
import com.splunk.shep.exporter.model.SplunkEvent;
/**
 * @author hyan
 *
 */
public class SplunkEventFormatter {
    private static final Logger log = Logger
	    .getLogger(SplunkEventFormatter.class);

    public static String mapToString(Map<String, String> map, OutputMode mode)
	    throws JsonGenerationException, JsonMappingException, IOException,
	    JAXBException {
	if (mode == json) {
	    return mapToJson(map);
	}
	if (mode == xml) {
	    return mapToXml(map);
	}
	return null;
    }

    public static String mapToXml(Map<String, String> map) throws JAXBException {
	SplunkEvent event = mapToObject(map);

	StringWriter writerTo = new StringWriter();
	JAXBContext jc = JAXBContext.newInstance(SplunkEvent.class);
	Marshaller marshaller = jc.createMarshaller();
	marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, false);
	marshaller.marshal(event, writerTo);
	return writerTo.toString();
    }

    public static String mapToJson(Map<String, String> map)
	    throws JsonGenerationException, JsonMappingException, IOException {
	Map<String, String> result = new HashMap<String, String>();
	if (map.containsKey(SPLUNK_EVENT_FIELD_RAW)) {
	    result.put(SPLUNK_EVENT_FIELD_RAW, map.get(SPLUNK_EVENT_FIELD_RAW));
	}
	if (map.containsKey(SPLUNK_EVENT_FIELD_SOURCE)) {
	    result.put(SPLUNK_EVENT_FIELD_SOURCE,
		    map.get(SPLUNK_EVENT_FIELD_SOURCE));
	}
	if (map.containsKey(SPLUNK_EVENT_FIELD_SOURCETYPE)) {
	    result.put(SPLUNK_EVENT_FIELD_SOURCETYPE,
		    map.get(SPLUNK_EVENT_FIELD_SOURCETYPE));
	}
	if (map.containsKey(SPLUNK_EVENT_FIELD_TIME)) {
	    result.put(SPLUNK_EVENT_FIELD_TIME,
		    map.get(SPLUNK_EVENT_FIELD_TIME));
	}
	return new ObjectMapper().writeValueAsString(result);
    }

    public static SplunkEvent mapToObject(Map<String, String> map) {
	SplunkEvent event = new SplunkEvent();
	event.setRaw(map.get(SPLUNK_EVENT_FIELD_RAW));
	event.setSourcetype(map.get(SPLUNK_EVENT_FIELD_SOURCETYPE));
	event.setSource(map.get(SPLUNK_EVENT_FIELD_SOURCE));
	event.setTime(map.get(SPLUNK_EVENT_FIELD_TIME));
	return event;
    }

    public static ShepExport mapsToObject(List<Map<String, String>> maps) {
	ShepExport export = new ShepExport();
	List<SplunkEvent> events = export.getSplunkEvent();
	for (Map<String, String> map : maps) {
	    events.add(mapToObject(map));
	}
	return export;
    }

    public static <T> T xmlToObject(String xmlStr, Class<T> clazz)
	    throws Exception {
	JAXBContext jc = JAXBContext.newInstance(clazz);
	Unmarshaller unmarshaller = jc.createUnmarshaller();
	Object target = unmarshaller.unmarshal(new StringReader(xmlStr));
	return clazz.cast(target);
    }

    public static <T> T jsonToObject(String jsonStr, Class<T> clazz)
	    throws Exception {
	// http://wiki.fasterxml.com/JacksonJAXBAnnotations
	ObjectMapper mapper = new ObjectMapper();
	AnnotationIntrospector intr = new JaxbAnnotationIntrospector();
	mapper.setAnnotationIntrospector(intr);
	mapper.configure(SerializationConfig.Feature.WRAP_ROOT_VALUE, false);
	return mapper.readValue(jsonStr, clazz);
    }

    public static <T> String objectToXml(Object target, Class<T> clazz)
	    throws Exception {
	JAXBContext jc = JAXBContext.newInstance(clazz);
	Marshaller marshaller = jc.createMarshaller();
	StringWriter writer = new StringWriter();
	marshaller.marshal(target, writer);
	String xml = writer.toString();
	log.debug("xml: " + xml);
	return xml;
    }

    public static <T> String objectToJson(Object target, Class<T> clazz)
	    throws Exception {
	ObjectMapper mapper = new ObjectMapper();
	AnnotationIntrospector intr = new JaxbAnnotationIntrospector();
	mapper.setAnnotationIntrospector(intr);
	mapper.configure(SerializationConfig.Feature.WRAP_ROOT_VALUE, false);
	StringWriter writer = new StringWriter();
	mapper.writeValue(writer, target);
	String json = writer.toString();
	log.debug("json: " + json);
	return json;
    }

    public static <T> String objectToString(Object target, Class<T> clazz,
	    OutputMode mode) throws Exception {
	if (mode == xml) {
	    return objectToXml(target, clazz);
	} else if (mode == json) {
	    return objectToJson(target, clazz);
	}
	return null;
    }

    // public static String normalizeSplunkJsonResult(String content)
    // throws XMLStreamException, IOException, JAXBException {
    // SplunkXMLStream parser = new SplunkXMLStream(
    // IOUtils.toInputStream(content));
    // HashMap<String, String> fieldMap = null;
    // StringBuilder sb = new StringBuilder();
    // sb.append("[");
    // do {
    // fieldMap = parser.nextResult();
    // sb.append(SplunkEventFormatter.toString(parser.nextResult(), json))
    // .append(",");
    // } while (fieldMap != null);
    // sb.deleteCharAt(sb.length() - 1);
    // sb.append("]");
    // return sb.toString();
    // }
}
