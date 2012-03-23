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
package com.splunk.shep.export;

import static com.splunk.shep.ShepConstants.SystemType.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.splunk.shep.ShepConstants.SystemType;
import com.splunk.shep.export.io.EventReader;
import com.splunk.shep.export.io.EventWriter;
import com.splunk.shep.server.model.ExportConf;
import com.splunk.shep.server.model.ExportConf.Channel;

/**
 * @author hyan
 *
 */
public class ChannelManager {

    private static ScheduledExecutorService service;
    private static Map<String, ScheduledFuture<Long>> futures;
    private static EventReader eventReader;
    private static EventWriter eventWriter;
    private static TranslogService translogService;

    private ChannelManager() {

    }

    public static void start() throws IOException {
	if (service == null) {
	    // TODO exposed as configurable parameter?
	    int corePoolSize = 5;
	    service = Executors.newScheduledThreadPool(corePoolSize);
	    futures = new HashMap<String, ScheduledFuture<Long>>();
	    eventReader = EventReader.getInstance(splunk);
	    translogService = new TranslogService();
	}

	// TODO get the list of index name and delays from configuration file
	ExportConf exportConf = new ExportConf();

	ChannelCallable cc;
	ScheduledFuture<Long> future;
	for (Channel channel : exportConf.getChannels()) {
	    cc = new ChannelCallable(channel.getIndexName(),
		    channel.getOutputMode(), exportConf.getOutputPath(),
		    channel.getOutputFileSystem(), exportConf.getTempPath());
	    future = service.schedule(cc, channel.getScheduleInterval(),
		    TimeUnit.SECONDS);
	    futures.put(channel.getIndexName(), future);
	}
    }

    public static void stop() {
	if (service != null) {
	    service.shutdown();
	}
    }

    public static boolean isCancelled(String indexName) {
	if (futures == null || !futures.containsKey(indexName)) {
	    return false;
	} else {
	    return futures.get(indexName).isCancelled();
	}
    }

    public static boolean isDone(String indexName) {
	if (futures == null || !futures.containsKey(indexName)) {
	    return false;
	} else {
	    return futures.get(indexName).isDone();
	}
    }

    public static boolean cancel(String indexName, boolean mayInterruptIfRunning) {
	if (futures == null || !futures.containsKey(indexName)) {
	    return false;
	} else {
	    return futures.get(indexName).cancel(mayInterruptIfRunning);
	}
    }
    private static class ChannelCallable implements Callable<Long> {
	private String indexName;
	// TODO currently splunk-java-sdk hardcoded outputMode, we will use
	// its enum once they have it
	private String outputMode;
	private String outputPath;
	private String outputFileSystem;
	private String tempPath;

	public ChannelCallable(String indexName, String outputMode,
		String outputPath, String outputFileSystem, String tempPath) {
	    this.indexName = indexName;
	    this.outputMode = outputMode;
	    this.outputPath = outputPath;
	    this.outputFileSystem = outputFileSystem;
	    this.tempPath = tempPath;
	}

	@Override
	public Long call() throws Exception {
	    SystemType type = type(outputFileSystem);

	    Map<String, Object> params = new HashMap<String, Object>();
	    params.put("output_mode", outputMode);
	    long lastEndTime = translogService.getEndTime(indexName);
	    InputStream is = eventReader.export(indexName, lastEndTime, params);

	    String fileName = String.format("%s_%d",
		    FileUtils.getFile(tempPath, indexName).getAbsolutePath(),
		    System.currentTimeMillis());
	    boolean append = true;
	    Configuration conf = null;
	    if (hdfs == type) {
		//TODO set up configuration
	    }
	    eventWriter = EventWriter.getInstance(type, fileName, append, conf);
	    eventWriter.write(is);
	    eventWriter.close();

	    // move finished files from temp dir to output dir. The idea is
	    // output dir only keep file finished file so that a separate MR job
	    // can use it while shep is running. Since hadoop takes everything
	    // in an input dir, having an empty DONE file indicating a file is
	    // finished might not work for hadoop hdfs, though it works for
	    // local files ystem.
	    FileUtils.moveFileToDirectory(new File(fileName), new File(
		    outputPath), false);

	    long endTime = eventReader.getEndTime();
	    translogService.setEndTime(indexName, endTime);

	    return endTime;
	}
	
    }
}
