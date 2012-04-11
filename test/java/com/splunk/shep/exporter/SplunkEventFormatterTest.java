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

import static com.splunk.shep.ShepConstants.OutputMode.*;
import static com.splunk.shep.ShepTestUtility.*;

import java.util.Map;

import org.apache.log4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.exporter.model.SplunkEvent;

/**
 * @author hyan
 *
 */
public class SplunkEventFormatterTest {

    private static final Logger log = Logger
	    .getLogger(SplunkEventFormatterTest.class);
    Map<String, String> fieldMap;

    @BeforeMethod(groups = "slow-unit")
    protected void setUp() throws Exception {
	SplunkEvent splunkEvent = getSplunkEvent("this is a line");
	fieldMap = getFieldMap(splunkEvent);
    }


    @Test(groups = "slow-unit")
    public void testToJson() throws Exception {
	String content = SplunkEventFormatter.mapToString(fieldMap, json);
	verifyFields(content, SplunkEvent.class, fieldMap, true, json);
    }

    @Test(groups = { "slow-unit" })
    public void testToXML() throws Exception {
	String content = SplunkEventFormatter.mapToString(fieldMap, xml);
	verifyFields(content, SplunkEvent.class, fieldMap, true, xml);
    }
}
