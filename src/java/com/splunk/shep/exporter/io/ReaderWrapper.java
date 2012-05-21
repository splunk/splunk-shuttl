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

package com.splunk.shep.exporter.io;

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

    private String prefix;
    private final String suffix;

    private Reader[] readers;
    private int rindex = 0; // start with the first reader

    public ReaderWrapper(String prefix, String suffix) {
	this.prefix = prefix;
	this.suffix = suffix;
    }

    public void wrapReader(Reader mainReader) {
	handlePossibleXMLVersionTag(mainReader);
	readers = new Reader[] { new StringReader(prefix), mainReader,
		new StringReader(suffix) };
    }

    private void handlePossibleXMLVersionTag(Reader mainReader) {
	String firstTag = getFirstTagOfReader(mainReader);
	if (!isXMLVersionTag(firstTag))
	    appendTagToPrefix(firstTag);
    }

    private void appendTagToPrefix(String firstTag) {
	prefix = prefix + firstTag;
    }

    private String getFirstTagOfReader(Reader mainReader) {
	int ch;
	String tag = "";
	while ((ch = getNextChar(mainReader)) != -1) {
	    tag += (char) ch;
	    if ((char) ch == '>')
		break;
	}
	return tag;
    }

    private int getNextChar(Reader mainReader) {
	try {
	    return mainReader.read();
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }

    private boolean isXMLVersionTag(String tag) {
	String trimmedTag = tag.trim();
	return trimmedTag.matches("<\\?xml version=[\"'].+?[\"'].*?\\?>");
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