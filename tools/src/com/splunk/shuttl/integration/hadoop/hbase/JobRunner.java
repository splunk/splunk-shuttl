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
package com.splunk.shuttl.integration.hadoop.hbase;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class JobRunner {
	private static HBaseAdmin admin;

	private static final String CONFIG_FILENAME = "csvmapper.filename";
	private static final String CONFIG_OUTPUTPATH = "csvmapper.outputPath";
	private static final String CONFIG_TABLENAME = "csvmapper.tableName";

	public JobRunner() {
	}

	public static void main(String[] args) throws Exception {
		
		Job job = CSVJobFactory.getConfiguredJob(args);
		
		Configuration jobConfiguration = job.getConfiguration();
		admin = new HBaseAdmin(jobConfiguration);
		
		JobRunner jobRunner = new JobRunner();
		jobRunner.run(job);
	}

	/**
	 * @param job
	 * @param jobConfiguration
	 * @param configuration
	 * @throws Exception
	 */
	private boolean run(Job job) throws Exception {

		Configuration configuration = job.getConfiguration();

		Path inputPath = new Path(configuration.get(CONFIG_FILENAME));
		Path outputPath = new Path(configuration.get(CONFIG_OUTPUTPATH));

		FileSystem fSystem = FileSystem.get(configuration);
		
		CreateHBaseTableIfNotExists(configuration.get(CONFIG_TABLENAME));

		DeleteOutputPathIfExists(fSystem, outputPath);

		FileSystem fs = FileSystem.get(configuration);
		String headerString = readFirstLine(fs.open(new Path("")));
		configuration.set(JobConfigurationConstants.HEADER_STRING, headerString);

		HTable hTable = new HTable(job.getConfiguration(),
				configuration.get(CONFIG_TABLENAME));

		// Auto configure partitioner and reducer
		HFileOutputFormat.configureIncrementalLoad(job, hTable);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		boolean complete = job.waitForCompletion(true);

		if (complete) {
			LoadIncrementalHFiles loader = new LoadIncrementalHFiles(configuration);
			loader.doBulkLoad(outputPath, hTable);
		}

		fSystem.deleteOnExit(outputPath);

		return complete;
	}

	/**
	 * @throws IOException
	 * 
	 */
	private void DeleteOutputPathIfExists(FileSystem fs, Path outputPath)
			throws IOException {
		if (fs.exists(outputPath))
				fs.delete(outputPath, true);
	}

	/**
	 * @throws IOException
	 * 
	 */
	private void CreateHBaseTableIfNotExists(String tableName)
			throws IOException {
		if (!admin.tableExists(tableName))
			admin.createTable(new HTableDescriptor(tableName));
	}

	private String readFirstLine(FSDataInputStream fsDataInputStream)
			throws IOException {
		DataInputStream in = new DataInputStream(fsDataInputStream);
		return new BufferedReader(new InputStreamReader(in)).readLine();
	}

}
