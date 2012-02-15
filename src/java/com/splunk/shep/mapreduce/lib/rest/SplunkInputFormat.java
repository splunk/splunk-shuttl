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
import java.io.InputStream;
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
import org.apache.log4j.Logger;

import com.splunk.Args;
import com.splunk.Job;
import com.splunk.Service;


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
    private static Logger logger = Logger.getLogger(SplunkInputFormat.class);

    protected class SplunkRecordReader implements RecordReader<LongWritable, V> {
	    private Class<V> inputClass;
	    private JobConf job;
	    private SplunkInputSplit split;
	    private long pos = 0;
	    private SplunkXMLStream parser = null;
        private Service service;
        private Job searchJob;
        private Args queryArgs;
        private int totalNumEvents = 0;
        private int currentEventOffset = 0;
        private int eventsLeftInChunk = 0;
        // for using export, instead of native search.
        private boolean usingExport = false;
        private InputStream stream;

        /**
         * @param split The InputSplit to read data for
         * @param inputClass input class
         * @param job Hadoop job
         */
        protected SplunkRecordReader(SplunkInputSplit split,
                      Class<V> inputClass, JobConf job) {
            this.inputClass = inputClass;
            this.split = split;
            this.job = job;
            queryArgs = new Args();
            logger.trace("split id " + split.getId());
            configureCredentials();
            addQueryParameters();
            initRecordReader();
        }

        private void initRecordReader() {
            String searchString = job.get(SplunkConfiguration.QUERY);
            if (!searchString.contains("search")) {
                searchString = "search "  + searchString;
            }
            if (usingExport) {
                stream = service.export(searchString, queryArgs);
            } else {
                queryArgs.put("exec_mode", "blocking"); // block until finished
                searchJob = service.getJobs().create(searchString, queryArgs);
                totalNumEvents = searchJob.getEventCount(); // total events
            }
        }

        private void configureCredentials() {
            Args args = new Args();
            args.put("username", job.get(SplunkConfiguration.USERNAME));
            args.put("password", job.get(SplunkConfiguration.PASSWORD));
            args.put("host", job.get(SplunkConfiguration.SPLUNKHOST));
            args.put("port", job.getInt(SplunkConfiguration.SPLUNKPORT, 8089));
            service = Service.connect(args);
        }

        private void addQueryParameters() {
    	    if (job.getInt(SplunkConfiguration.INDEXBYHOST, 0) == 0) {
                queryArgs.put(SplunkConfiguration.TIMEFORMAT, job
		    		       .get(SplunkConfiguration.TIMEFORMAT));
            }
		    if (job.get(SplunkConfiguration.STARTTIME + split.getId()) != null) {
                queryArgs.put(SplunkConfiguration.STARTTIME, job
				           .get(SplunkConfiguration.STARTTIME + split.getId()));
		    }
		    if (job.get(SplunkConfiguration.ENDTIME + split.getId()) != null) {
                queryArgs.put(SplunkConfiguration.ENDTIME, job
				           .get(SplunkConfiguration.ENDTIME + split.getId()));
		    }
        }

        private SplunkXMLStream getCurrentParser() {
            if (usingExport) {
                if (parser == null) {
                    try {
                        parser = new SplunkXMLStream(stream);
                    }
                    catch (Exception e) {
                        throw new RuntimeException("Failed to retrieve results stream");
                    }
                }
            } else {

                // hide multi-pumping the events reader when we have a large
                // number nof events returned by a search.
                // ASSUMPTION: we get called only once per Splunk EVENT

                int chunkSize = 40000;
                if (eventsLeftInChunk == 0) {
                    int eventsLeft = totalNumEvents - currentEventOffset;
                    eventsLeftInChunk = (eventsLeft > chunkSize)
                            ? chunkSize : eventsLeft;
                    try {
                        Args args = new Args();
                        args.put("offset", currentEventOffset);
                        args.put("count", eventsLeftInChunk);
                        parser = new SplunkXMLStream(searchJob.getResults(args));
                    }
                    catch (Exception e) {
                        throw new RuntimeException("Failed to retrieve results stream");
                    }
                }

                currentEventOffset++;
                eventsLeftInChunk--;
            }

            assert(parser != null);
            return parser;
        }

        @Override
        public void close() throws IOException {
            searchJob.cancel();
            service.logout();
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
            return pos;
        }

        @Override
        public float getProgress() throws IOException {
            return pos / split.getLength();
        }

        @Override
        public boolean next(LongWritable key, V value) throws IOException {

            parser = getCurrentParser();
            logger.trace("next");
            pos++;
            HashMap<String, String> map = parser.nextResult();
            if (map == null)
                return false;
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
            host = "localhost";
            id = 0;
            locations = new String[1];
            locations[0] = host;
        }

        public String getHost() {
            return host;
        }

        public void setLocations(String[] hosts) {
            locations = hosts;
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
            return endtime;
        }

        public void setEndtime(String endtime) {
            this.endtime = endtime;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            id = in.readInt();
            host = in.readUTF();
            int numlocations = in.readInt();
            locations = new String[numlocations];
            for (int i = 0; i < numlocations; i++) {
                locations[i] = in.readUTF();
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(id);
            out.writeUTF(host);
            out.writeInt(locations.length);
            for (String location : locations) {
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
            return locations;
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
		                                    job);
	    }
        catch (Exception ex) {
	        throw new IOException(ex.getMessage());
	    }
    }

    @Override
    public SplunkInputSplit[] getSplits(JobConf job, int arg1)
            throws IOException {
        // number of splits is equal to the number of time ranges provided for
        // the search
        SplunkInputSplit splits[] = new SplunkInputSplit[job.getInt(
                SplunkConfiguration.NUMSPLITS, 1)];
        logger.trace("num splits foo"
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
