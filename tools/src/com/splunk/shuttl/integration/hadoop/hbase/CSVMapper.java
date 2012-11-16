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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CSVMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

	private CSVToContextPreparator preparator;

	/**
	 * @param keyProvider
	 */
	public CSVMapper(CSVToContextPreparator csvPreparator) {
		this.preparator = csvPreparator;
	}

	/**
	 * 
	 */
	public CSVMapper() {
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		Configuration configuration = context.getConfiguration();

		if (inputValueEqualsHeaders(value.toString(),
				configuration.get(JobConfigurationConstants.HEADER_STRING)))
			return;

		this.preparator = CSVToContextPreparatorFactory.create(configuration);

		List<KeyValue> results = preparator
				.prepareForContext(key, value.toString());

		for (KeyValue result : results)
			context.write(new ImmutableBytesWritable(result.getKey()), result);
	}

	/**
	 * @param value
	 * @param header
	 * @return
	 */
	private boolean inputValueEqualsHeaders(String value, String header) {
		return value.equals(header);
	}
}