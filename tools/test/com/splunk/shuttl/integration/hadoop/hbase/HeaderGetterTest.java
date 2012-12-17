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

import java.util.List;

import org.testng.annotations.Test;

import au.com.bytecode.opencsv.CSVParser;


/**
 * @author mklich
 *
 */
@Test(groups = { "fast-unit" })
public class HeaderGetterTest {

	@Test(expectedExceptions = Exception.class)
	public void getHeaders_headerRowContainingEmptyHeaderName_castsException()
			throws Exception {
		String stringContainingRowWithEmptyHeaderName = "header1,,header3,header4";

		new HeaderGetter(new CSVParser(),
				stringContainingRowWithEmptyHeaderName).getHeaders();

	}

	@Test(expectedExceptions = RuntimeException.class)
	public void getHeaders_emptyHeaderRow_throwsException() throws Exception {
		String stringWithEmptyHeader = "";

		new HeaderGetter(new CSVParser(), stringWithEmptyHeader)
				.getHeaders();
	}

	public void getHeaders_twoHeadersZeroValueRows_arrayWithTwoStrings()
			throws Exception {
		String headerString = "header1,header2";

		List<String> headers = new HeaderGetter(new CSVParser(), headerString)
				.getHeaders();

		assertEquals(2, headers.size());
		assertEquals("header1", headers.get(0));
		assertEquals("header2", headers.get(1));
	}
}
