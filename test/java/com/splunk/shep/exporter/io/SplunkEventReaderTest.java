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
package com.splunk.shep.exporter.io;

import static com.splunk.shep.ShepConstants.OutputMode.*;
import static com.splunk.shep.ShepTestUtility.*;
import static com.splunk.shep.exporter.io.SplunkEventReader.*;

import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.splunk.Service;
import com.splunk.shep.ShepConstants.OutputMode;
import com.splunk.shep.exporter.TranslogService;
import com.splunk.shep.exporter.model.ShepExport;

/**
 * @author hyan
 *
 */
public class SplunkEventReaderTest {
    private static final Logger log = Logger
	    .getLogger(SplunkEventReaderTest.class);

    Service service;
    EventReader eventReader;
    TranslogService translogService;

    @BeforeClass(groups = { "integration" })
    protected void setUp() throws Exception {
	initTestEnv();

	service = createService();
	eventReader = new SplunkEventReader();
	translogService = new TranslogService();
    }

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
    public void testExportJsonOnce() throws Exception {
	testExportOnce(json);
    }

    @Test(groups = { "integration" })
    public void testExportXmlOnce() throws Exception {
	testExportOnce(xml);
    }

    @Test(groups = { "integration" })
    public void testExportJsonTwice() throws Exception {
	testExportTwice(json);
    }

    @Test(groups = { "integration" })
    public void testExportXmlTwice() throws Exception {
	testExportTwice(xml);
    }

    private void testExportOnce(OutputMode mode) throws Exception {
	deleteTranslog();
	ShepExport shepExport = getShepExport("this is a line", 2);
	testExport(shepExport, mode);
    }

    private void testExportTwice(OutputMode mode) throws Exception {
	deleteTranslog();

	ShepExport shepExport = getShepExport("this is a export 1 line", 2);
	testExport(shepExport, mode);

	sleep(TIME_GAP * 1000);

	shepExport = getShepExport("this is a export 2 line", 2);
	testExport(shepExport, mode);
    }

    private void testExport(ShepExport shepExport, OutputMode mode)
	    throws Exception {

	addOneShot(service, TEST_INDEX_NAME, shepExport);

	long lastEndTime = translogService.getEndTime(TEST_INDEX_NAME);
	InputStream is = eventReader.export(TEST_INDEX_NAME, lastEndTime, mode);
	String actualContent = IOUtils.toString(is);
	log.debug("actualContent from Splunk Server: " + actualContent);

	verifyFields(actualContent, shepExport, mode);

	translogService.setEndTime(TEST_INDEX_NAME, eventReader.getEndTime());
	is.close();
    }

}
