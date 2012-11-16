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

import static org.testng.AssertJUnit.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.LongWritable;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import au.com.bytecode.opencsv.CSVParser;


@Test(groups = { "fast-unit" })
public class CSVToContextPreparatorTest {
	private CSVToContextPreparator preparator;

	private final LongWritable anyKey = new LongWritable(17);
	private final String anyColumnFamily = "columnFamily";
	private final String anyQualifier = "qualifier";

	private final String anyFilename = "anyFilename";

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeMethod
	public void setUp() throws Exception {
		preparator = new CSVToContextPreparator(new CSVParser(),
				new HBaseKeyGenerator(""), anyColumnFamily, Arrays.asList(anyQualifier));
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterMethod
	public void tearDown() throws Exception {

	}

	public void prepareForContext_keyAndAnyMultipleValues_keyStaysTheSame()
			throws IOException {
		Configuration configuration = createTestConfiguration(3);
		
		CSVToContextPreparator customPreparator = CSVToContextPreparatorFactory
				.create(configuration);
		
		List<KeyValue> result = customPreparator.prepareForContext(anyKey,
 "value,value2,value3");

		assertEquals(result.get(0).getRow(), result.get(1).getRow());
		assertEquals(result.get(1).getRow(), result.get(2).getRow());
	}

	private Configuration createTestConfiguration(int numberOfHeaders) {
		Configuration configuration = new Configuration();
		configuration.set(JobConfigurationConstants.COLUMN_FAMILY, anyColumnFamily);
		configuration.set(JobConfigurationConstants.HEADER_STRING,
				createHeaderString(numberOfHeaders));
		configuration.set(JobConfigurationConstants.FILENAME, anyFilename);

		return configuration;
	}

	private String createHeaderString(int numberOfHeaders) {
		if (numberOfHeaders == 0)
			return "";
		if (numberOfHeaders == 1)
			return anyQualifier;
		return anyQualifier.concat(","
				.concat(createHeaderString(numberOfHeaders - 1)));
	}

	public void prepareForContext_emptyText_EmptyArrayReturned()
			throws IOException {

		List<KeyValue> result = preparator.prepareForContext(anyKey, "");

		assertEquals(0, result.size());
	}

	public void prepareForContext_oneValueColumn_OneValueIsPreparedForContext()
			throws IOException {
		String inputText = "value1";
		List<KeyValue> result = preparator.prepareForContext(anyKey, inputText);

		assertEquals(1, result.size());
		assertEquals(result.get(0).getValue(), inputText.getBytes());
	}

	public void prepareForContext_threeValueColumns_ThreeValuesArePreparedForContext()
			throws IOException {

		CSVToContextPreparator customPreparator = CSVToContextPreparatorFactory
				.create(createTestConfiguration(3));

		String inputText = "value1,value2,value3";
		List<KeyValue> result = customPreparator.prepareForContext(anyKey,
				inputText);

		assertEquals(3, result.size());
		assertEquals(result.get(0).getValue(), "value1".getBytes());
		assertEquals(result.get(1).getValue(), "value2".getBytes());
		assertEquals(result.get(2).getValue(), "value3".getBytes());
	}
	
	public void prepareForContext_columnFamilyAndQualifier_customColumnFamilyAndQualifier()
			throws IOException {
		String customFamily = "columnFamily";
		String customQualifier = "qualifier";
		CSVToContextPreparator contextPreparator = new CSVToContextPreparator(
				new CSVParser(), new HBaseKeyGenerator(""), customFamily,
				Arrays.asList(customQualifier));

		List<KeyValue> result = contextPreparator.prepareForContext(anyKey,
				"value1,value2");
		
		System.out.println(result.get(0).getFamily().toString());
		assertEquals(result.get(0).getFamily(), customFamily.getBytes());
		assertEquals(result.get(0).getQualifier(), customQualifier.getBytes());
	}
}
