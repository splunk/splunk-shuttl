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
package com.splunk.shep.mapred.lib.rest.mock;

import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.splunk.Args;
import com.splunk.Service;
import com.splunk.shep.mapred.lib.rest.SplunkConfiguration;
import com.splunk.shep.mapred.lib.rest.SplunkInputFormat;
import com.splunk.shep.mapred.lib.rest.SplunkWritable;
import com.splunk.shep.mapred.lib.rest.SplunkXMLStream;

/**
 * @author hyan
 *
 */
public class SplunkInputFormatMock<V extends SplunkWritable> extends
	SplunkInputFormat<V> {
    private static Log LOG = LogFactory.getLog(SplunkInputFormatMock.class);
    public static final String QUERY1 = "search index=main source=*wordfile-timestamp";
    public static final String QUERY2 = "search index=main source=*wordfile-timestamp  17:04:15";
    public static final String MAIN_INDEX = "main";
    private static Map<String, List<HashMap<String, String>>> splunkIndices = new HashMap<String, List<HashMap<String, String>>>();

    public static void oneShotDataToSplunk(String index,
	    List<HashMap<String, String>> values) {
	splunkIndices.put(index, values);
    }

    public static HashMap<String, String> mockValue(String text, String time)
		throws ParseException {
	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	String raw = time + " " + text;
	SimpleDateFormat format1 = new SimpleDateFormat(
		"yyyy-MM-dd HH:mm:ss.SSS zzz");
	time = format1.format(format.parse(time));

	long currentTime = System.currentTimeMillis();
	HashMap<String, String> value = new HashMap<String, String>();
	value.put("_sourcetype", "wordfile-timestamp-too_small");
	value.put("index", "main");
	value.put("host", "localhost");
	// value.put("_cd", cd);
	// value.put("_serial", serial);
	value.put("_si", "localhost,main");
	value.put("splunk_server", "localhost");
	value.put("linecount", "1");
	value.put("_indextime", "" + currentTime / 1000);
	value.put("_raw", raw);
	value.put("source", "wordfile-timestamp");
	value.put("_time", time);
	value.put("sourcetype", "wordfile-timestamp-too_small");

	LOG.debug("value: " + value);
	return value;
    }

    protected class SplunkRecordReaderMock extends SplunkRecordReader {
	protected SplunkRecordReaderMock(
		com.splunk.shep.mapred.lib.rest.SplunkInputFormat.SplunkInputSplit split,
		Class<V> inputClass, JobConf job) {
	    super(split, inputClass, job);
	}

	@Override
	protected Service getService(Args args) {
	    return mockService(args);
	}

	@Override
	protected SplunkXMLStream getParser(InputStream stream) {
	    try {
		return mockParser();
	    } catch (Exception e) {
		throw new RuntimeException("Failed to retrieve results stream");
	    }
	}

	private Service mockService(Args args) {
	    Service service = mock(Service.class);
	    when(service.export(anyString(), anyMap())).thenReturn(
		    mock(InputStream.class));
	    when(service.logout()).thenReturn(service);
	    return service;
	}

	private SplunkXMLStream mockParser() throws IOException, ParseException {
	    String query = job.get(SplunkConfiguration.QUERY);
	    parser = mock(SplunkXMLStream.class);
	    if (QUERY1.equals(query)) {
		HashMap<String, String>[] values = (HashMap<String, String>[]) splunkIndices
			.get(MAIN_INDEX).toArray();
		HashMap<String, String> value0 = values[0];
		HashMap[] moreValues = new HashMap[values.length];
		for (int i = 1; i < values.length; i++) {
		    moreValues[i - 1] = values[i];
		}
		moreValues[values.length - 1] = null;
		when(parser.nextResult()).thenReturn(value0, moreValues);
	    } else if (QUERY2.equals(query)) {
		when(parser.nextResult()).thenReturn(
			splunkIndices.get(MAIN_INDEX).get(0), null);
	    } else {
		throw new IOException(
			"unsupport test search query, add the mock object to mockParser method before testing this search scenario");
	    }

	    return parser;
	}

    }

    @Override
    public RecordReader<LongWritable, V> getRecordReader(InputSplit split,
	    JobConf job, Reporter arg2) throws IOException {
	try {
	    Class inputClass = Class.forName(job
		    .get(SplunkConfiguration.SPLUNKEVENTREADER));
	    return new SplunkRecordReaderMock((SplunkInputSplit) split,
		    inputClass, job);
	} catch (Exception ex) {
	    LOG.error(ex);
	    throw new IOException(ex.getMessage());
	}
    }

}
