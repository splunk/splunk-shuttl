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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.LongWritable;

import au.com.bytecode.opencsv.CSVParser;

/**
 * @author mklich
 */
public class CSVToContextPreparator {

	private CSVParser csvParser;
	private HBaseKeyGenerator keyGen;
	private String columnFamily;
	private List<String> headers;

	/**
	 * @param qualifier
	 * @param columnFamiliy
	 * 
	 */
	public CSVToContextPreparator(CSVParser parser,
			HBaseKeyGenerator keyGenerator, String columnFamily, List<String> headers) {
		this.csvParser = parser;
		this.keyGen = keyGenerator;
		this.columnFamily = columnFamily;
		this.headers = headers;
	}

	/**
	 * @param key
	 * @param csvInput
	 * @return
	 * @throws IOException
	 */
	public List<KeyValue> prepareForContext(LongWritable key, String csvInput)
			throws IOException {
		List<KeyValue> result = new ArrayList<KeyValue>();

		if (csvInput.trim().equals(""))
			return Arrays.asList();

		List<String> values = Arrays.asList(csvParser.parseLine(csvInput));

		result = getKeyValueArrayFromCSVValues(keyGen.getKey(key), values);
		return result;
	}

	/**
	 * @param key
	 * @param values
	 * @return
	 */
	private List<KeyValue> getKeyValueArrayFromCSVValues(String key,
			List<String> values) {
		List<KeyValue> result = new ArrayList<KeyValue>();

		for (int i = 0; i < headers.size(); i++)
			result.add(i, new KeyValue(key.getBytes(), columnFamily.getBytes(),
					headers.get(i).getBytes(), values.get(i).getBytes()));
		return result;
	}
}