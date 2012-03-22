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

import static com.splunk.shep.ShepConstants.*;
import static com.splunk.shep.ShepConstants.SystemType.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.splunk.Args;
import com.splunk.EntityCollection;
import com.splunk.Index;
import com.splunk.Service;
import com.splunk.shep.export.TranslogService;

/**
 * @author hyan
 *
 */
@Test(groups = { "integration" })
public class SplunkEventReaderTest {
    public static final String PWD = System.getProperty("user.dir");
    // public static final String indexName = "shepTestIndex";
    public static final String indexName = "testshep_index";
    private static final Logger log = Logger
	    .getLogger(SplunkEventReaderTest.class);

    private void wait_event_count(Index index, int value, int seconds) {
	while (seconds > 0) {
	    try {
		Thread.sleep(1000); // 1000ms (1 second sleep)
		seconds = seconds - 1;
		if (index.getTotalEventCount() == value) {
		    return;
		}
		index.refresh();
	    } catch (InterruptedException e) {
		return;
	    } catch (Exception e) {
		return;
	    }
	}
    }

    @BeforeClass
    public void setUp() {

    }

    @AfterClass
    public void tearDown() {

    }

    private void addOneShot() throws IOException {
	// File file = FileUtils.getFile(PWD, "build-cache", "splunk");
	// if (System.getProperty(SPLUNK_HOME_PROPERTY) == null) {
	// System.setProperty(SPLUNK_HOME_PROPERTY, file.getAbsolutePath());
	// }

	File translog = new File(TRANSLOG_FILE_PATH);
	if (translog.exists()) {
	    FileUtils.forceDelete(translog);
	}

	// create test index
	Args args = new Args();
	args.put("username", "admin");
	args.put("password", "changeme");
	args.put("host", "localhost");
	args.put("port", 8089);
	Service service = Service.connect(args);
	EntityCollection<Index> indexes = service.getIndexes();
	// if (!indexes.containsKey(indexName)) {
	// indexes.create(indexName);
	// indexes.refresh();
	// }

	assertTrue(indexes.containsKey(indexName));
	Index index = indexes.get(indexName);
	index.clean(60);
	assertEquals(index.getTotalEventCount(), 0);
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	String date = sdf.format(new Date());
	index.submit(date + "Hello World. \u0150");
	index.submit(date + "Goodbye world. \u0150");
	wait_event_count(index, 2, 30);
	assertEquals(index.getTotalEventCount(), 2);
    }

    @Test
    public void testDisjunction() {
	SplunkEventReader eventReader = new SplunkEventReader();
	long lastEndTime = TranslogService.DEFAULT_ENDTIME;
	String disjunction = eventReader.disjunction(lastEndTime);
	long endTime = eventReader.getEndTime();
	log.debug(String.format("disjunction between %d and %d:%s",
		lastEndTime, endTime, disjunction));

	lastEndTime = endTime;
	disjunction = eventReader.disjunction(lastEndTime);
	endTime = eventReader.getEndTime();
	log.debug(String.format("disjunction between %d and %d:%s",
		lastEndTime, endTime, disjunction));
    }

    @Test
    public void testExport() throws IllegalArgumentException, IOException {
	addOneShot();

	EventReader eventReader = EventReader.getInstance(splunk);
	TranslogService translogService = new TranslogService();
	// String indexName = "_internal";
	Map<String, Object> params = new HashMap<String, Object>();
	params.put("output_mode", "json");
	long lastEndTime = translogService.getEndTime();
	InputStream is = eventReader.export(indexName, lastEndTime, params);
	System.out.println("is: " + is);
	String events = IOUtils.toString(is);
	System.out.println("events: " + events);
	assertEquals("[]", events);
	translogService.setEndTime(eventReader.getEndTime());

	lastEndTime = translogService.getEndTime();
	is = eventReader.export(indexName, lastEndTime, params);
	events = IOUtils.toString(is);
	System.out.println("events: " + events);

    }
}
