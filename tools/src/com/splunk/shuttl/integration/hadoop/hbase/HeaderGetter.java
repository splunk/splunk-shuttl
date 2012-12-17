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

import java.util.Arrays;
import java.util.List;

import au.com.bytecode.opencsv.CSVParser;

/**

 *
 */
public class HeaderGetter {

	private CSVParser parser;
	private String headerString;

	/**
	 * @param parser
	 * @param headerString
	 */
	public HeaderGetter(CSVParser parser, String headerString) {
		this.parser = parser;
		this.headerString = headerString;
	}

	public List<String> getHeaders() {
		try {
			return doGetHeaders(parser, headerString);
		} catch (Exception e) {
			throw new HeaderNotFoundException(e.getMessage());
		}
	}

	/**
	 * @param csvParser
	 * @param tmpFile
	 * @return
	 * @throws Exception
	 */
	private List<String> doGetHeaders(CSVParser csvParser, String headerString)
			throws Exception {

		List<String> headers = Arrays.asList(csvParser.parseLine(headerString));

		if (emptyHeadersNameExists(headers))
			throw new HeaderNotFoundException("Error in header string: "
					+ headerString);

		return headers;
	}

	private boolean emptyHeadersNameExists(List<String> headers) {
		for (String header : headers)
			if (header.equals(""))
				return true;

		return false;
	}

	public class HeaderNotFoundException extends RuntimeException {

		/**
		 * @param string
		 */
		public HeaderNotFoundException(String string) {
			super(string);
		}

		private static final long serialVersionUID = 1L;
	}
}

