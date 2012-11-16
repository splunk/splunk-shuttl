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

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import au.com.bytecode.opencsv.CSVParser;

/**
 * @author mklich
 * 
 */
public class CSVToContextPreparatorFactory {

	/**
	 * @param configuration
	 * @param parser
	 * @param headers
	 * @return
	 */
	public static CSVToContextPreparator create(Configuration configuration) {
		CSVParser parser = new CSVParser();

		String headerString = configuration
				.get(JobConfigurationConstants.HEADER_STRING);
		String columnFamily = configuration
				.get(JobConfigurationConstants.COLUMN_FAMILY);

		List<String> headers = new HeaderGetter(parser, headerString).getHeaders();

		HBaseKeyGenerator keyGenerator = new HBaseKeyGenerator(
				configuration.get(JobConfigurationConstants.FILENAME));
		return new CSVToContextPreparator(parser, keyGenerator, columnFamily,
				headers);
	}

}
