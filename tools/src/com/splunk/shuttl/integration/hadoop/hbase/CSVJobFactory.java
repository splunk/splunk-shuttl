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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class CSVJobFactory {


	private CSVJobFactory() {
	}

	/**
	 * @return the hadoopConfiguration
	 * @throws IOException
	 */
	public static Job getConfiguredJob(String[] arguments)
			throws IOException {

		Configuration jobConfiguration = new Configuration(true);
		// Load hbase-site.xml
		HBaseConfiguration.addHbaseResources(jobConfiguration);

		jobConfiguration.set("fs.default.name", arguments[0]);
		jobConfiguration.set("mapred.job.tracker", arguments[1]);
		jobConfiguration.set(JobConfigurationConstants.FILENAME, arguments[2]);
		jobConfiguration.set(JobConfigurationConstants.OUTPUT_PATH, arguments[3]);
		jobConfiguration.set(JobConfigurationConstants.TABLE_NAME, arguments[4]);

		jobConfiguration.set(JobConfigurationConstants.COLUMN_FAMILY, "d");


		Job job = new Job(jobConfiguration, "BucketToHbase");
		job.setJarByClass(CSVMapper.class);

		job.setMapperClass(CSVMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);

		job.setInputFormatClass(TextInputFormat.class);

		return job;
	}
}
