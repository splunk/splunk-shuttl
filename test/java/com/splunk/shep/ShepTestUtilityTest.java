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

import static com.splunk.shep.ShepConstants.OutputMode.*;
import static com.splunk.shep.ShepTestUtility.*;
import static com.splunk.shep.exporter.SplunkEventFormatter.*;
import static org.testng.Assert.*;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.splunk.shep.exporter.model.ShepExport;
import com.splunk.shep.exporter.model.SplunkEvent;

/**
 * @author hyan
 *
 */
public class ShepTestUtilityTest {
    String expectedSplunkEventJson;
    String expectedSplunkEventXml;
    String expectedShepExportJson;
    String expectedShepExportXml;
    SplunkEvent splunkEvent;
    ShepExport shepExport;
    Map<String, String> fieldMap;

    static Logger log = Logger.getLogger(ShepTestUtilityTest.class);

    @BeforeClass(groups = { "fast-unit" })
    protected void setUp() throws Exception {
	splunkEvent = getSplunkEvent("this is a line");
	shepExport = getShepExport("this is a line", 2);
	expectedSplunkEventJson = getExpectedSplunkEventJson(splunkEvent);
	expectedSplunkEventXml = getExpectedSplunkEventXml(splunkEvent, true);
	expectedShepExportJson = getExpectedShepExportJson(shepExport);
	expectedShepExportXml = getExpectedShepExportXml(shepExport);
	fieldMap = getFieldMap(splunkEvent);
    }

    @Test(groups={"fast-unit"})
    public void testObjectToJson() throws Exception {
	String actual = objectToJson(splunkEvent, SplunkEvent.class);
	assertEquals(actual, expectedSplunkEventJson);

	List<SplunkEvent> splunkEvents = shepExport.getSplunkEvent();
	// actual = objectToJson(shepExport, ShepExport.class);
	actual = objectToJson(splunkEvents, splunkEvents.getClass());
	assertEquals(actual, expectedShepExportJson);
    }

    @Test(groups = { "fast-unit" })
    public void testObjectToXml() throws Exception {
	String actual = objectToXml(splunkEvent, SplunkEvent.class);
	assertEquals(actual, expectedSplunkEventXml);
	actual = objectToXml(shepExport, ShepExport.class);
	assertEquals(actual, expectedShepExportXml);
    }

    @Test(groups = { "fast-unit" })
    public void testVerifyFieldsNotNull() throws Exception {
	verifyFieldsNotNull(expectedSplunkEventXml, SplunkEvent.class, true, xml,
		SHEP_EXPORT_EVENT_FIELDS);
	verifyFieldsNotNull(expectedSplunkEventJson, SplunkEvent.class, true, json,
		SHEP_EXPORT_EVENT_FIELDS);
    }

    @Test(groups = { "fast-unit" })
    public void testVerifyFields() throws Exception {
	verifyFields(expectedSplunkEventXml, SplunkEvent.class, fieldMap, true, xml);
	verifyFields(expectedSplunkEventJson, SplunkEvent.class, fieldMap, true, json);
    }

}
