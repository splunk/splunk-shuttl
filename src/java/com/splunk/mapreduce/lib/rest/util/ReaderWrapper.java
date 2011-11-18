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

package com.splunk.mapreduce.lib.rest.util;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

/**
 * This class is needed because the Splunk results in xml do not have a root
 * element By wrapping the http response with some string prefixes it is
 * possible to inject a root
 * 
 * @author kpakkirisamy and periksson
 * 
 */
public class ReaderWrapper extends Reader {
	private Reader[] readers;
	private int rindex = 0; // start with the first reader

	public ReaderWrapper(Reader[] readers) {
		this.readers = readers;
	}

	public ReaderWrapper(String prefix, String suffix) {
		this(new Reader[] { new StringReader(prefix), null,
				new StringReader(suffix) });
	}

	public void wrapReader(Reader mainReader) {
		String readerContent = new ContentReader(mainReader).getContent();
		String content = getContentWithoutXMLTag(readerContent);
		readers[1] = new StringReader(content);
	}

	private String getContentWithoutXMLTag(String readerContent) {
		String xmlTagEnd = "?>";
		int indexOfXMLTagEnd = readerContent.indexOf(xmlTagEnd);
		if (indexOfXMLTagEnd == -1)
			return readerContent;
		else
			return readerContent.substring(indexOfXMLTagEnd
					+ xmlTagEnd.length());
	}

	@Override
	public void close() throws IOException {
		for (Reader reader : readers)
			reader.close();
	}

	@Override
	public int read(char[] buf, int offset, int length) throws IOException {
		int charsread = -1;
		do {
			charsread = readers[this.rindex].read(buf, offset, length);
			if (charsread == -1) {
				this.rindex++;
			}
		} while (charsread == -1 && this.rindex < readers.length);
		return charsread;
	}

}