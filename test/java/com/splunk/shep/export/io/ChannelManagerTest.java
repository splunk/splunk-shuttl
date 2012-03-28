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
import static com.splunk.shep.ShepConstants.OutputMode.*;
import static com.splunk.shep.ShepConstants.SystemType.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import com.splunk.shep.ShepTestBase;
import com.splunk.shep.export.ChannelManager;
import com.splunk.shep.server.model.ExportConf;
import com.splunk.shep.server.model.ExportConf.Channel;

/**
 * @author hyan
 *
 */
public class ChannelManagerTest extends ShepTestBase {
    private static final Logger log = Logger
	    .getLogger(ChannelManagerTest.class);

    @Test(groups = { "integration" })
    public void testStart() throws IOException {
	deleteTranslog();
	String outputPath = initOutputDir();

	// create index1 and add two events
	String indexName1 = "shepTestIndex1".toLowerCase();
	String[] testEvents1 = prefixTime(new String[] { "this is event 11",
		"this is event 12" });
	addOneShot(indexName1, testEvents1);

	// create index2 and add two events
	String indexName2 = "shepTestIndex2".toLowerCase();
	String[] testEvents2 = prefixTime(new String[] { "this is event 21",
		"this is event 22" });
	addOneShot(indexName2, testEvents2);

	// create exportConf, can get it from config file
	ExportConf exportConf = new ExportConf();
	exportConf.setTempPath(TEMP_DIR_PATH);
	exportConf.setOutputPath(outputPath);

	List<Channel> channels = new ArrayList<Channel>();
	exportConf.setChannels(channels);
	Channel channel1 = new Channel();
	channel1.setIndexName(indexName1);
	channel1.setOutputFileSystem(local.toString());
	channel1.setOutputMode(json.toString());
	channel1.setScheduleInterval(120);
	channels.add(channel1);

	Channel channel2 = new Channel();
	channel2.setIndexName(indexName2);
	channel2.setOutputFileSystem(local.toString());
	channel2.setOutputMode(json.toString());
	channel2.setScheduleInterval(120);
	channels.add(channel2);

	ChannelManager manager = new ChannelManager();
	manager.setExportConf(exportConf);
	manager.start();

	int count = 0;
	while (count < 10
		&& (!manager.isDone(indexName1) || !manager.isDone(indexName2))) {
	    log.debug("still working, sleep for 1 second...");
	    sleep(1000);
	    count++;
	}
	assertTrue(manager.isDone(indexName1));
	assertTrue(manager.isDone(indexName2));

	File output = new File(outputPath);
	for (File f : output.listFiles()) {
	    if (f.getName().startsWith(indexName1)) {
		verifyJson(IOUtils.toString(new FileReader(f)), testEvents1);
	    } else if (f.getName().startsWith(indexName2)) {
		verifyJson(IOUtils.toString(new FileReader(f)), testEvents2);
	    }
	}
    }

    private String initOutputDir() throws IOException {
	File output = FileUtils.getFile(SHEP_HOME, "output");
	if (output.exists()) {
	    FileUtils.forceDelete(output);
	}
	output.mkdir();
	return output.getAbsolutePath();
    }
}
