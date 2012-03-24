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
package com.splunk.shep.export.io;

import static com.splunk.shep.ShepConstants.SystemType.*;
import static com.splunk.shep.export.io.SplunkEventReader.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import com.splunk.shep.ShepTestBase;
import com.splunk.shep.export.TranslogService;

/**
 * @author hyan
 *
 */
public class SplunkEventReaderTest extends ShepTestBase {
    private static final Logger log = Logger
	    .getLogger(SplunkEventReaderTest.class);

    // @Test
    public void testDisjunction() {
	SplunkEventReader eventReader = new SplunkEventReader();
	long lastEndTime = TranslogService.DEFAULT_ENDTIME;
	String disjunction = eventReader.disjunction(lastEndTime);
	long endTime = eventReader.getEndTime();
	log.debug(String.format("disjunction between %d and %d:%s",
		lastEndTime, endTime, disjunction));

	lastEndTime = endTime + 1;
	disjunction = eventReader.disjunction(lastEndTime);
	endTime = eventReader.getEndTime();
	log.debug(String.format("disjunction between %d and %d:%s",
		lastEndTime, endTime, disjunction));
    }

    @Test(groups = { "integration" })
    public void testExport() throws IllegalArgumentException, IOException {
	String[] testEvents = prefixTime(new String[] { "this is event 1",
		"this is event 2" });
	addOneShot(testEvents);

	EventReader eventReader = EventReader.getInstance(splunk);
	TranslogService translogService = new TranslogService();
	Map<String, Object> params = new HashMap<String, Object>();
	params.put("output_mode", "json");
	long lastEndTime = translogService.getEndTime(indexName);
	InputStream is = eventReader.export(indexName, lastEndTime, params);
	verifyJson(is, testEvents);
	translogService.setEndTime(indexName, eventReader.getEndTime());

	sleep(TIME_GAP * 1000);

	testEvents = prefixTime(new String[] { "this is event 3",
		"this is event 4" });
	addOneShot(testEvents);

	lastEndTime = translogService.getEndTime(indexName);
	is = eventReader.export(indexName, lastEndTime, params);
	verifyJson(is, testEvents);
	translogService.setEndTime(indexName, eventReader.getEndTime());

    }

}
