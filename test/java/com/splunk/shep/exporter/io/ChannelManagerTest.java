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

import static com.splunk.shep.ShepConstants.*;
import static com.splunk.shep.ShepConstants.OutputMode.*;
import static com.splunk.shep.ShepConstants.SystemType.*;
import static com.splunk.shep.ShepTestUtility.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.splunk.Service;
import com.splunk.shep.ShepConstants.OutputMode;
import com.splunk.shep.ShepConstants.SystemType;
import com.splunk.shep.exporter.ChannelManager;
import com.splunk.shep.exporter.model.ShepExport;
import com.splunk.shep.server.model.ExporterConf;
import com.splunk.shep.server.model.ExporterConf.Channel;

/**
 * @author hyan
 *
 */
public class ChannelManagerTest {
    private static final Logger log = Logger
	    .getLogger(ChannelManagerTest.class);

    private Service service;
    private String outputPath;

    @BeforeClass(groups = { "integration" })
    protected void setUp() throws Exception {
	service = createService();
	initTestEnv();

	deleteTranslog();
	outputPath = getOutputPath();
    }

    @Test(groups = { "integration" })
    public void testStart() throws Exception {
	// create index1 and add two events
	String indexName1 = "shepTestIndex1".toLowerCase();
	ShepExport shepExport1 = getShepExport("this is export 1 line", 2);
	addOneShot(service, indexName1, shepExport1);

	// create index2 and add two events
	String indexName2 = "shepTestIndex2".toLowerCase();
	ShepExport shepExport2 = getShepExport("this is export 2 line", 2);
	addOneShot(service, indexName2, shepExport2);

	Channel channel1 = getChannel(indexName1, local, json, 120);
	Channel channel2 = getChannel(indexName2, local, xml, 120);
	ExporterConf conf = getExporterConf();
	conf.getChannels().add(channel1);
	conf.getChannels().add(channel2);

	ChannelManager manager = new ChannelManager();
	manager.setExportConfiguration(conf);
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
	File[] files = output.listFiles(new ExportFileFilter(json));
	assertEquals(files.length, 1);
	String actualContent = readFile(files[0]);
	log.debug("actualContent from output file: " + actualContent);

	output = new File(outputPath);
	files = output.listFiles(new ExportFileFilter(xml));
	assertEquals(files.length, 1);
	actualContent = readFile(files[0]);
	log.debug("actualContent from output file: " + actualContent);
    }

    private ExporterConf getExporterConf() {
	// create exportConf, can get it from config file
	ExporterConf exportConf = new ExporterConf();
	exportConf.setTempPath(TEMP_DIR_PATH);
	exportConf.setOutputPath(outputPath);

	List<Channel> channels = new ArrayList<Channel>();
	exportConf.setChannels(channels);
	return exportConf;
    }

    private Channel getChannel(String indexName, SystemType type,
	    OutputMode mode, long interval) {
	Channel channel = new Channel();
	channel.setIndexName(indexName);
	channel.setOutputFileSystem(type.toString());
	channel.setOutputMode(mode.toString());
	channel.setScheduleInterval(interval);
	return channel;
    }

    private String getOutputPath() throws IOException {
	File output = FileUtils.getFile(SHEP_HOME, "output");
	if (output.exists()) {
	    FileUtils.forceDelete(output);
	}
	output.mkdir();
	return output.getAbsolutePath();
    }

    private static class ExportFileFilter implements FileFilter {
	private String ext;

	public ExportFileFilter(OutputMode mode) {
	    this.ext = mode.toString();
	}

	public boolean accept(File file) {
	    if (file.getName().endsWith(ext)) {
		return true;
	    }
	    return false;
	}
    }
}
