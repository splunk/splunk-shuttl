package com.splunk.shep.mapreduce.lib.rest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import com.splunk.Args;
import com.splunk.Service;

/**
 * This InputFormat uses a search to pull data from Splunk No progress is
 * reported as the search results size is unknown as it is streaming.
 * 
 * @author kpakkirisamy, hyan
 * 
 * @param <K>
 * @param <V>
 */
public class SplunkInputFormat<V extends SplunkWritable> extends
	InputFormat<LongWritable, V> {
    private static final Logger LOG = Logger.getLogger(SplunkInputFormat.class);

    protected class SplunkRecordReader extends RecordReader<LongWritable, V> {
	private Class<V> inputClass;
	private SplunkInputSplit split;
	private long pos = 0;
	private LongWritable key;
	private V value;

	protected Configuration conf;
	protected SplunkXMLStream parser = null;
	protected Service service;
	protected Args queryArgs;
	protected InputStream stream;

	/**
	 * @param split
	 *            The InputSplit to read data for
	 * @param inputClass
	 *            input class
	 * @param conf
	 *            Hadoop Configuration
	 */
	public SplunkRecordReader(SplunkInputSplit split,
		Class<V> inputClass, Configuration conf) {
	    this.inputClass = inputClass;
	    this.split = split;
	    this.conf = conf;
	    queryArgs = new Args();
	    LOG.trace("split id " + split.getId());
	    configureCredentials();
	    addQueryParameters();
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
		throws IOException, InterruptedException {
	    String searchString = conf.get(SplunkConfiguration.QUERY);
	    if (!searchString.contains("search")) {
		searchString = "search " + searchString;
		conf.set(SplunkConfiguration.QUERY, searchString);
	    }
	    stream = service.export(searchString, queryArgs);
	}

	@Override
	public void close() throws IOException {
	    service.logout();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
		InterruptedException {
	    return key;
	}

	@Override
	public V getCurrentValue() throws IOException, InterruptedException {
	    return value;
	}

	@Override
	public float getProgress() throws IOException {
	    return pos / split.getLength();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
	    key = new LongWritable();
	    value = ReflectionUtils.newInstance(inputClass, conf);

	    if (parser == null) {
		parser = getParser(stream);
	    }
	    pos++;
	    HashMap<String, String> map = parser.nextResult();
	    if (map == null) {
		return false;
	    }
	    value.setMap(map);
	    return true;
	}

	protected Service getService(Args args) {
	    return Service.connect(args);
	}

	protected SplunkXMLStream getParser(InputStream stream) {
	    try {
		return new SplunkXMLStream(stream);
	    } catch (Exception e) {
		LOG.error(e);
		throw new RuntimeException("Failed to retrieve results stream");
	    }
	}

	private void configureCredentials() {
	    Args args = new Args();
	    args.put("username", conf.get(SplunkConfiguration.USERNAME));
	    args.put("password", conf.get(SplunkConfiguration.PASSWORD));
	    args.put("host", conf.get(SplunkConfiguration.SPLUNKHOST));
	    args.put("port", conf.getInt(SplunkConfiguration.SPLUNKPORT, 8089));
	    service = getService(args);
	}

	private void addQueryParameters() {
	    if (conf.getInt(SplunkConfiguration.INDEXBYHOST, 0) == 0) {
		queryArgs.put(SplunkConfiguration.TIMEFORMAT,
			conf.get(SplunkConfiguration.TIMEFORMAT));
	    }
	    if (conf.get(SplunkConfiguration.STARTTIME + split.getId()) != null) {
		queryArgs
			.put(SplunkConfiguration.STARTTIME,
				conf.get(SplunkConfiguration.STARTTIME
					+ split.getId()));
	    }
	    if (conf.get(SplunkConfiguration.ENDTIME + split.getId()) != null) {
		queryArgs.put(SplunkConfiguration.ENDTIME,
			conf.get(SplunkConfiguration.ENDTIME + split.getId()));
	    }
	}
    }

    protected static class SplunkInputSplit extends InputSplit implements
	    Writable {
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
    public List<InputSplit> getSplits(JobContext context) throws IOException,
	    InterruptedException {
	// number of splits is equal to the number of time ranges provided for
	// the search
	Configuration conf = context.getConfiguration();
	List<InputSplit> splits = new ArrayList<InputSplit>();
	LOG.trace("num splits foo"
		+ conf.getInt(SplunkConfiguration.NUMSPLITS, 1));
	SplunkInputSplit split;
	for (int i = 0; i < conf.getInt(SplunkConfiguration.NUMSPLITS, 1); i++) {
	    split = new SplunkInputSplit();
	    splits.add(split);
	    split.setId(i);
	    if (conf.getInt(SplunkConfiguration.INDEXBYHOST, 0) == 0) {
		// search by query
		split.setHost(conf.get(SplunkConfiguration.SPLUNKHOST));
		if (conf.get(SplunkConfiguration.STARTTIME + i) != null) {
		    split.setStarttime(conf
			    .get(SplunkConfiguration.STARTTIME + i));
		}
		if (conf.get(SplunkConfiguration.ENDTIME + i) != null) {
		    split.setEndtime(conf.get(SplunkConfiguration.ENDTIME
			    + i));
		}
	    } else {
		// search by indexers
		split.setHost(conf.get(SplunkConfiguration.INDEXHOST + i));
	    }

	    JobConf job = new JobConf(conf);
	    split.setLocations(getClusterNodeList(job));
	    resetLocations(split, split.getHost()); // currently splits
	    // will run only on
	    // the same indexer
	    // if co-located
	    // with a Hadoop
	    // node
	}
	return splits;
    }

    @Override
    public RecordReader<LongWritable, V> createRecordReader(InputSplit split,
	    TaskAttemptContext context) throws IOException,
	    InterruptedException {
	try {
	    Configuration conf = context.getConfiguration();
	    Class inputClass = Class.forName(conf
		    .get(SplunkConfiguration.SPLUNKEVENTREADER));
	    return new SplunkRecordReader((SplunkInputSplit) split, inputClass,
		    conf);
	} catch (Exception ex) {
	    LOG.error(ex);
	    throw new IOException(ex.getMessage());
	}
    }

    private void resetLocations(SplunkInputSplit split, String host) {
	String locations[] = split.getLocations();
	for (String location : locations) {
	    if (location.equals(host)) {
		LOG.trace("resetting getLocations with " + host);
		split.setLocations(new String[] { host });
		break;
	    }
	}
    }

    private String[] getClusterNodeList(JobConf job) throws IOException {
	JobClient client = new JobClient(job);
	ClusterStatus status = client.getClusterStatus(true);
	Collection<String> activeTrackerNames = status.getActiveTrackerNames();
	LOG.trace("num active trackers " + activeTrackerNames.size());
	String locations[] = new String[activeTrackerNames.size()];
	int index = 0;
	for (String host : status.getActiveTrackerNames()) {
	    LOG.trace("activeTracker " + host);
	    StringTokenizer st = new StringTokenizer(host, ":");
	    while (st.hasMoreElements()) {
		String ehost = st.nextToken();
		if (ehost.startsWith("tracker_")) {
		    locations[index] = ehost.substring("tracker_".length());
		    LOG.trace("adding cluser location " + locations[index]);
		    index++;
		}
	    }
	}
	return locations;
    }

}
