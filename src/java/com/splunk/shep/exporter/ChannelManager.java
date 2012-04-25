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

import static com.splunk.shep.ShepConstants.SystemType.*;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.splunk.shep.ShepConstants.OutputMode;
import com.splunk.shep.ShepConstants.SystemType;
import com.splunk.shep.exporter.io.EventReader;
import com.splunk.shep.exporter.io.EventWriter;
import com.splunk.shep.exporter.io.SplunkEventReader;
import com.splunk.shep.server.model.ExporterConf.Channel;
import com.splunk.shep.server.model.ExporterConfiguration;
import com.splunk.shep.server.services.SplunkExporterService;

/**
 * @author hyan
 *
 */
public class ChannelManager implements SplunkExporterService {

    private final Logger log = Logger.getLogger(ChannelManager.class);

    private ScheduledExecutorService service = null;;
    private Map<String, Boolean> status = new ConcurrentHashMap<String, Boolean>();
    private EventReader eventReader = null;
    private TranslogService translogService = null;
    private ExporterConfiguration exportConf = null;

    public ChannelManager() {

    }

    @Override
    public String getStatus() {
	if (service == null) {
	    return STOPPED;
	}
	return RUNNING;
    }

    @Override
    public void start() throws IOException {
	if (service == null) {
	    // TODO exposed as configurable parameter?
	    int corePoolSize = 5;
	    service = Executors.newScheduledThreadPool(corePoolSize);
	    if (translogService == null) {
			translogService = new TranslogService();
	    }
	}

	ChannelWorker worker;
	if (exportConf.getChannels() != null) {
	    for (Channel channel : exportConf.getChannels()) {
		log.debug("Configuring Export for Channel/Index: "
			+ channel.getIndexName());
		worker = new ChannelWorker(channel.getIndexName(),
			channel.getOutputMode(), exportConf.getOutputPath(),
			channel.getOutputFileSystem(), exportConf.getTempPath());
		service.scheduleWithFixedDelay(worker, 0,
			channel.getScheduleInterval(), TimeUnit.SECONDS);
	    }
	} else {
	    log.debug("No channels configured");
	}
    }

    @Override
    public void stop() {
	if (service != null) {
	    service.shutdown();
	}
    }

    public boolean isDone(String indexName) {
	return status.containsKey(indexName) && status.get(indexName);
    }

    private class ChannelWorker implements Runnable {
	private String indexName;
	private String outputMode;
	private String outputPath;
	private String outputFileSystem;
	private String tempPath;
	private EventWriter eventWriter;

	public ChannelWorker(String indexName, String outputMode,
		String outputPath, String outputFileSystem, String tempPath) {
	    this.indexName = indexName;
	    this.outputMode = outputMode;
	    this.outputPath = outputPath;
	    this.outputFileSystem = outputFileSystem;
	    this.tempPath = tempPath;
	}

	@Override
	public void run() {
	    status.put(indexName, false);

	    SystemType type = valueOf(outputFileSystem);
	    EventReader eventReader = new SplunkEventReader();
	    
	    long lastEndTime = translogService.getEndTime(indexName);
	    OutputMode mode = OutputMode.valueOf(outputMode);
	    try {
		log.debug("Starting export for Channel: " + indexName);
		SimpleDateFormat sdf = new SimpleDateFormat(
			"yyyy.MM.dd.HH.mm.ss");
		String timeStr = sdf.format(new Date());
		String fileName = String.format("%s_%s.%s",
			FileUtils.getFile(tempPath, indexName)
				.getAbsolutePath(), timeStr, outputMode);
		boolean append = true;
		Configuration conf = null;
		if (hdfs == type) {
		    // TODO set up configuration
		}
		EventWriter eventWriter = EventWriter.getInstance(type,
			fileName, append, conf);
		String events = eventReader.nextEvents(indexName, lastEndTime,
			mode, 1000);
		eventWriter.write(events);
		eventWriter.close();

		// move finished files from temp dir to output dir. The idea is
		// output dir only keep file finished file so that a separate MR
		// job
		// can use it while shep is running. Since hadoop takes
		// everything
		// in an input dir, having an empty DONE file indicating a file
		// is
		// finished might not work for hadoop hdfs, though it works for
		// local files ystem.
		FileUtils.moveFileToDirectory(new File(fileName), new File(
			outputPath), false);

		long endTime = eventReader.getEndTime();
		translogService.setEndTime(indexName, endTime);
	    } catch (Exception e) {
		log.error("Failed to run channel thread: ", e);
	    }

	    status.put(indexName, true);
	    log.debug(Thread.currentThread().getName() + " is done");
	}
	
    }

    public ExporterConfiguration getExportConfiguration() {
	return exportConf;
    }

    public void setExportConfiguration(ExporterConfiguration exportConf) {
	this.exportConf = exportConf;
    }

    public TranslogService getTranslogService() {
	return translogService;
    }

    public void setTranslogService(TranslogService translogService) {
	this.translogService = translogService;
    }

}
