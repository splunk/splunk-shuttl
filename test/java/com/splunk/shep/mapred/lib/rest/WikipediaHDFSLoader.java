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

package com.splunk.shep.mapred.lib.rest;

import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.tools.bzip2.CBZip2InputStream;

import edu.jhu.nlp.wikipedia.WikiXMLParser;
import edu.jhu.nlp.wikipedia.WikiXMLSAXParser;

/**
 * 
 * @author Kiru Pakkirisamy (based on a sample by Jason Smith)
 * 
 */
public class WikipediaHDFSLoader {

    /**
     * This method is a test method to check whether the data was loaded
     * correctly in to HDFS
     * 
     * @param args
     *            String hdfs file path
     */
    public static void main2(String args[]) {
	if (args.length != 1) {
	    System.err.println("Usage: Parser  <hdfsfilepath>");
	    System.exit(-1);
	}

	WikiPageSAXHandler handler = new WikiPageSAXHandler(args[0], true);
	org.apache.hadoop.io.Text key = new org.apache.hadoop.io.Text();
	org.apache.hadoop.io.MapWritable value = new org.apache.hadoop.io.MapWritable();
	try {
	    while (handler.next(key, value)) {
		org.apache.hadoop.io.Text titlekey = new org.apache.hadoop.io.Text();
		titlekey.set("title");
		System.out.println("title " + value.get(titlekey));

		org.apache.hadoop.io.Text textkey = new org.apache.hadoop.io.Text();
		textkey.set("author");
		System.out.println("author " + value.get(textkey));

		org.apache.hadoop.io.Text timekey = new org.apache.hadoop.io.Text();
		timekey.set("modifiedtime");
		System.out.println("modifiedtime " + value.get(timekey));

	    }

	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
	if (args.length != 2) {
	    System.err
		    .println("Usage: Parser <url-to-wikipedia-bz2-file> <hdfsfilepath>");
	    System.exit(-1);
	}
	try {
	    WikiPageSAXHandler handler = new WikiPageSAXHandler(args[1]);
	    URL dumpurl = new URL(args[0]);
	    URLConnection uc = dumpurl.openConnection();
	    InputStream is = uc.getInputStream();
	    byte[] ignoreBytes = new byte[2];
	    is.read(ignoreBytes); // "B", "Z" bytes from commandline tools
	    WikiXMLParser wxsp = new WikiXMLSAXParser(new CBZip2InputStream(is));
	    wxsp.setPageCallback(handler);
	    wxsp.parse();
	    handler.close();
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }
}
