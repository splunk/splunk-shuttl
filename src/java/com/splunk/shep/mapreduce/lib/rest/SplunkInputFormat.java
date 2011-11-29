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

package com.splunk.shep.mapreduce.lib.rest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.AbstractHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;

import com.splunk.shep.mapreduce.lib.rest.util.HttpClientUtils;

/**
 * This InputFormat uses a search to pull data from Splunk No progress is
 * reported as the search results size is unknown as it is streaming
 * 
 * @author kpakkirisamy
 * 
 * @param <V>
 *            subclass of SplunkWritable
 */
public class SplunkInputFormat<V extends SplunkWritable> implements
		InputFormat<LongWritable, V>, JobConfigurable {
	private static Logger logger = logger = Logger
			.getLogger(SplunkInputFormat.class);

	protected class SplunkRecordReader implements RecordReader<LongWritable, V> {
		private Class<V> inputClass;
		private JobConf job;
		private SplunkInputSplit split;
		private HttpClient httpclient = null;
		private long pos = 0;
		private SplunkXMLStream parser = null;
		private String host;
		private ArrayList<NameValuePair> qparams;

		/**
		 * @param split
		 *            The InputSplit to read data for
		 * @throws SQLException
		 */
		protected SplunkRecordReader(SplunkInputSplit split,
				Class<V> inputClass, JobConf job, HttpClient httpClient) {
			this.inputClass = inputClass;
			this.split = split;
			this.host = split.getHost();
			this.job = job;
			logger.trace("split id " + split.getId());
			this.httpclient = httpClient;
			this.qparams = new ArrayList<NameValuePair>();
			initRecordReader();
		}

		private void initRecordReader() {
			configureCredentials();
			addQParams();
			HttpResponse response = executeHttpGetRequest();
			setParserWithHttpResponse(response);
		}

		private void setParserWithHttpResponse(HttpResponse response) {
			if (isOKHttpRequestStatus(response))
				this.parser = getSplunkXMLStream (response);
			else
				throw new RuntimeException("Bad Status:"
						+ response.getStatusLine());
		}

		private void configureCredentials() {
			((AbstractHttpClient) httpclient).getCredentialsProvider()
					.setCredentials(
							new AuthScope(host, job.getInt(
									SplunkConfiguration.SPLUNKPORT,
									SplunkConfiguration.SPLUNK_DEFAULT_PORT)),
							new UsernamePasswordCredentials(job
									.get(SplunkConfiguration.USERNAME), job
									.get(SplunkConfiguration.PASSWORD)));
		}

		private void addQParams() {
			// keys itself are the exact strings to be passed to Splunk
			qparams.add(new BasicNameValuePair(SplunkConfiguration.QUERY,
					SplunkConfiguration.SEARCHSTR
							+ job.get(SplunkConfiguration.QUERY)));
			if (job.getInt(SplunkConfiguration.INDEXBYHOST, 0) == 0) {
				qparams.add(new BasicNameValuePair(
						SplunkConfiguration.TIMEFORMAT, job
								.get(SplunkConfiguration.TIMEFORMAT)));
				if (job.get(SplunkConfiguration.STARTTIME + split.getId()) != null) {
					qparams.add(new BasicNameValuePair(
							SplunkConfiguration.STARTTIME, job
									.get(SplunkConfiguration.STARTTIME
											+ split.getId())));
				}
				if (job.get(SplunkConfiguration.ENDTIME + split.getId()) != null) {
					qparams.add(new BasicNameValuePair(
							SplunkConfiguration.ENDTIME, job
									.get(SplunkConfiguration.ENDTIME
											+ split.getId())));
				}
			}
		}

		private HttpResponse executeHttpGetRequest() {
			try {
				return httpclient.execute(getHttpGet());
			} catch (ClientProtocolException e) {
				throw new RuntimeException(e);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		private HttpUriRequest getHttpGet() {
			URI uri = getURI();
			logger.debug("GET: " + uri);
			System.out.println("URI: " + uri);
			return new HttpGet(uri);
		}

		private URI getURI() {
			try {
				return URIUtils.createURI("https", host,
						job.getInt(SplunkConfiguration.SPLUNKPORT, 8089),
						SplunkConfiguration.SPLUNK_SEARCH_URL,
						URLEncodedUtils.format(qparams, "UTF-8"), null);
			} catch (URISyntaxException e) {
				throw new RuntimeException(e);
			}
		}

		private boolean isOKHttpRequestStatus(HttpResponse response) {
			return response.getStatusLine().getStatusCode() == 200;
		}

		private SplunkXMLStream getSplunkXMLStream(HttpResponse response) {
			try {
				return new SplunkXMLStream(response.getEntity()
						.getContent());
			} catch (IllegalStateException e) {
				throw new RuntimeException(e);
			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			this.httpclient.getConnectionManager().shutdown();
		}

		@Override
		public LongWritable createKey() {
			return new LongWritable();
		}

		@Override
		public V createValue() {
			return ReflectionUtils.newInstance(inputClass, job);
		}

		@Override
		public long getPos() throws IOException {
			// TODO Auto-generated method stub
			return this.pos;
		}

		@Override
		public float getProgress() throws IOException {
			return this.pos / this.split.getLength();
		}

		@Override
		public boolean next(LongWritable key, V value) throws IOException {
			logger.trace("next");
			this.pos++;
			HashMap<String, String> map = this.parser.nextResult();
			if (map == null) {
				return false;
			}
			value.setMap(map);
			return true;
		}
	}

	protected static class SplunkInputSplit implements InputSplit {
		private String host;
		private int id;
		private String starttime;
		private String endtime;
		private String locations[];

		public SplunkInputSplit() {
			this.host = "localhost";
			this.id = 0;
			this.locations = new String[1];
			this.locations[0] = this.host;
		}

		public String getHost() {
			return host;
		}

		public void setLocations(String[] hosts) {
			this.locations = hosts;
		}

		public void setHost(String host) {
			this.host = host;
		}

		public String getStarttime() {
			return starttime;
		}

		public void setStarttime(String starttime) {
			this.starttime = starttime;
		}

		public String getEndtime() {
			return this.endtime;
		}

		public void setEndtime(String endtime) {
			this.endtime = endtime;
		}

		public int getId() {
			return this.id;
		}

		public void setId(int id) {
			this.id = id;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.id = in.readInt();
			this.host = in.readUTF();
			int numlocations = in.readInt();
			this.locations = new String[numlocations];
			for (int i = 0; i < numlocations; i++) {
				this.locations[i] = in.readUTF();
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(this.id);
			out.writeUTF(this.host);
			out.writeInt(locations.length);
			for (String location : this.locations) {
				out.writeUTF(location);
			}
		}

		@Override
		public long getLength() throws IOException {
			// we dont know the size of search results
			return Long.MAX_VALUE;
		}

		@Override
		public String[] getLocations() {
			return this.locations;
		}

	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public RecordReader<LongWritable, V> getRecordReader(InputSplit split,
			JobConf job, Reporter arg2) throws IOException {
		try {
			Class inputClass = Class.forName(job
					.get(SplunkConfiguration.SPLUNKEVENTREADER));
			return new SplunkRecordReader((SplunkInputSplit) split, inputClass,
					job, getHttpClient());
		} catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
	}

	private HttpClient getHttpClient() {
		try {
			return HttpClientUtils.getHttpClient();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public SplunkInputSplit[] getSplits(JobConf job, int arg1)
			throws IOException {
		// number of splits is equal to the number of time ranges provided for
		// the search
		SplunkInputSplit splits[] = new SplunkInputSplit[job.getInt(
				SplunkConfiguration.NUMSPLITS, 1)];
		logger.trace("num splits "
				+ job.getInt(SplunkConfiguration.NUMSPLITS, 1));
		for (int i = 0; i < job.getInt(SplunkConfiguration.NUMSPLITS, 1); i++) {
			splits[i] = new SplunkInputSplit();
			splits[i].setId(i);
			if (job.getInt(SplunkConfiguration.INDEXBYHOST, 0) == 0) {
				// search by query
				splits[i].setHost(job.get(SplunkConfiguration.SPLUNKHOST));
				if (job.get(SplunkConfiguration.STARTTIME + i) != null) {
					splits[i].setStarttime(job
							.get(SplunkConfiguration.STARTTIME + i));
				}
				if (job.get(SplunkConfiguration.ENDTIME + i) != null) {
					splits[i].setEndtime(job.get(SplunkConfiguration.ENDTIME
							+ i));
				}
			} else {
				// search by indexers
				splits[i].setHost(job.get(SplunkConfiguration.INDEXHOST + i));
			}
			splits[i].setLocations(getClusterNodeList(job));
			resetLocations(splits[i], splits[i].getHost()); // currently splits
															// will run only on
															// the same indexer
															// if co-located
															// with a Hadoop
															// node
		}
		return splits;
	}

	private void resetLocations(SplunkInputSplit split, String host) {
		String locations[] = split.getLocations();
		for (String location : locations) {
			if (location.equals(host)) {
				logger.trace("resetting getLocations with " + host);
				split.setLocations(new String[] { host });
				break;
			}
		}
	}

	private String[] getClusterNodeList(JobConf job) throws IOException {
		JobClient client = new JobClient(job);
		ClusterStatus status = client.getClusterStatus(true);
		logger.trace("num active trackers " + status.getTaskTrackers());
		String locations[] = new String[status.getTaskTrackers()];
		int index = 0;
		for (String host : status.getActiveTrackerNames()) {
			logger.trace("activeTracker " + host);
			StringTokenizer st = new StringTokenizer(host, ":");
			while (st.hasMoreElements()) {
				String ehost = st.nextToken();
				if (ehost.startsWith("tracker_")) {
					locations[index] = ehost.substring("tracker_".length());
					logger.trace("adding cluser location " + locations[index]);
					index++;
				}
			}
		}
		return locations;
	}

}
