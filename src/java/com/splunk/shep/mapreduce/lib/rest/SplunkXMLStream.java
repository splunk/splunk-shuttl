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

package com.splunk.shep.mapreduce.lib.rest;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.log4j.Logger;

import com.splunk.shep.mapreduce.lib.rest.util.ReaderWrapper;

/**
 * This returns the results of a splunk search as a map of name-value pairs
 *
 * @author kpakkirisamy
 */
public class SplunkXMLStream {
    static final String RESULT = "result";
    static final String FIELD = "field";
    static final String KEY = "k";
    static final String V = "v";
    static final String TEXT = "text";
    private XMLEventReader eventReader;

    private static Logger logger = Logger.getLogger(SplunkXMLStream.class);

    public SplunkXMLStream(InputStream in) throws Exception {
        XMLInputFactory inputFactory = XMLInputFactory.newInstance();
        inputFactory.setProperty(XMLInputFactory.IS_COALESCING, true);
        inputFactory.setProperty(XMLInputFactory.IS_VALIDATING, Boolean.FALSE);
        // create a wrapped reader to inject a SplunkResult root element
        String prefix = "<?xml version='1.0' encoding='UTF-8'?> <SplunkResult>";
        String suffix = "</SplunkResult>";
        ReaderWrapper readerWrapper = new ReaderWrapper(prefix, suffix);
        readerWrapper.wrapReader(new InputStreamReader(in));
        this.eventReader = inputFactory.createXMLEventReader(readerWrapper);
    }

    public HashMap<String, String> nextResult() throws IOException {
        try {
            String key = null;
            HashMap<String, String> map = null;
            while (eventReader.hasNext()) {
                XMLEvent event = eventReader.nextEvent();
                if (event.isStartElement()) {
                    StartElement startElement = event.asStartElement();
                    logger.trace("startElement "
                            + startElement.getName().getLocalPart());
                    if (startElement.getName().getLocalPart() == (RESULT)) {
                        map = new HashMap<String, String>();
                    }
                    if (event.asStartElement().getName().getLocalPart()
                            .equals(FIELD)) {
                        Iterator<Attribute> attributes = startElement
                                .getAttributes();
                        while (attributes.hasNext()) {
                            Attribute attribute = attributes.next();
                            if (attribute.getName().toString().equals(KEY)) {
                                key = attribute.getValue();
                            }
                        }
                    }
                    if (event.asStartElement().getName().getLocalPart()
                            .equals(TEXT)
                            || event.asStartElement().getName().getLocalPart()
                            .equals(V)) {
                        event = eventReader.nextEvent();
                        String value = event.asCharacters().getData();
                        if (map.get(key) != null) {
                            value = map.get(key) + "," + value;
                        }
                        map.put(key, value);
                        logger.trace("key " + key + " value " + value);
                        continue;
                    }
                }
                if (event.isEndElement()) { // If we reach the end of a result
                    // element we return
                    EndElement endElement = event.asEndElement();
                    logger.trace("endElement "
                            + endElement.getName().getLocalPart());
                    if (endElement.getName().getLocalPart().equals(RESULT)) {
                        if (map.isEmpty()) {
                            // other empty results element
                            return null;
                        } else {
                            return map;
                        }
                    }
                }
            }
        } catch (XMLStreamException e) {
            logger.error(e);
            throw new IOException(e);
        }
        return null; // end of elements
    }

}