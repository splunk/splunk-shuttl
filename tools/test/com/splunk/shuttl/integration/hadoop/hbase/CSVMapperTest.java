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

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.integration.hadoop.hbase.HeaderGetter.HeaderNotFoundException;

@Test(groups = { "fast-unit" })
public class CSVMapperTest {

	CSVMapper mapper;
	Context context;
	Configuration configuration;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeMethod
	public void setUp() throws Exception {
		mapper = new CSVMapper();
		context = mock(Context.class);
		configuration = mock(Configuration.class);

		when(context.getConfiguration()).thenReturn(configuration);
		when(configuration.get(JobConfigurationConstants.HEADER_STRING))
				.thenReturn("DefaultHeader,DefaultHeader2,DefaultHeader3");
		when(configuration.get(JobConfigurationConstants.COLUMN_FAMILY))
				.thenReturn("defaultColumnFamily");
		when(configuration.get(JobConfigurationConstants.FILENAME)).thenReturn(
				"DefaultFilename.csv");

	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterMethod
	public void tearDown() throws Exception {

	}

	public void map_emptyText_DoesNotWriteAnythingToContext() throws IOException,
			InterruptedException {

		LongWritable offsetKey = new LongWritable(17);
		Text emptyCSVRow = new Text("");

		mapper.map(offsetKey, emptyCSVRow, context);

		verify(context, times(0)).write(any(), any());
	}

	public void map_inputStringIsHeaderLineFromFile_DoesNotWriteHeaderDataToContext()
			throws IOException, InterruptedException {
		Text headerStringText = new Text("header1,header2,header3");
		LongWritable anyKey = new LongWritable(17);

		when(configuration.get(JobConfigurationConstants.HEADER_STRING))
				.thenReturn(headerStringText.toString());

		mapper.map(anyKey, headerStringText, context);

		verify(context, never()).write(any(), any());
	}

	@Test(expectedExceptions = { HeaderNotFoundException.class })
	public void map_headerContainsEmptyName_castsException() throws IOException,
			InterruptedException {
		Text inputValue = new Text("value1,value2,value3,value4");

		when(configuration.get(JobConfigurationConstants.HEADER_STRING))
				.thenReturn("h1,,h3,h4");

		LongWritable anyKey = new LongWritable(17);

		mapper.map(anyKey, inputValue, context);
	}

	private File createTempFile(String fileContent) throws IOException {
		File tmpFile = File.createTempFile("tempTestFile", "csv");
		BufferedWriter bw = new BufferedWriter(new FileWriter(tmpFile));
		bw.write(fileContent);
		bw.close();
		return tmpFile;
	}
}
