package com.splunk.shep.mapreduce.lib.rest;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.mapreduce.lib.rest.tests.WikiLinkCount;
import com.splunk.shep.testutil.FileSystemUtils;
import com.splunk.shep.testutil.HadoopFileSystemPutter;

public class SplunkEventReaderTest {

    private static final String TEST_INPUT_FILENAME_1 = "sdata1";
    private static final String TEST_INPUT_FILENAME_2 = "sdata2";
    private HadoopFileSystemPutter putter;

    @BeforeMethod(groups = { "slow" })
    public void setUp() {
	FileSystem fileSystem = FileSystemUtils.getLocalFileSystem();
	putter = HadoopFileSystemPutter.get(fileSystem);
    }

    @AfterMethod(groups = { "slow" })
    public void tearDown() {
	putter.deleteMyFiles();
    }

    @Test(groups = { "slow" })
    public void theTest() throws IOException {
	putFilesOnHadoop();

	runMapReduceJob();
    }

    private void putFilesOnHadoop() {
	File file1 = getFirstFile();
	File file2 = getSecondFile();
	putter.putFile(file1);
	putter.putFile(file2);
    }

    private File getFirstFile() {
	return getFileForFileName(TEST_INPUT_FILENAME_1);
    }

    private File getSecondFile() {
	return getFileForFileName(TEST_INPUT_FILENAME_2);
    }

    private File getFileForFileName(String fileName) {
	return new File(MapReduceRestTestConstants.TEST_RESOURCES_PATH + "/"
		+ fileName);
    }

    private void runMapReduceJob() throws IOException {

	JobConf conf = new JobConf(WikiLinkCount.class);
	conf.setJobName(this.getClass().getSimpleName());
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(NullWritable.class);

	conf.setMapperClass(Map.class);

	conf.setInputFormat(com.splunk.shep.mapreduce.lib.rest.SplunkEventsInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	Path pathForFirst = putter.getPathForFile(getFirstFile());
	Path pathForSecondFile = putter.getPathForFile(getSecondFile());
	FileInputFormat.setInputPaths(conf, pathForFirst, pathForSecondFile);
	FileOutputFormat.setOutputPath(conf, getOutputPath());

	JobClient.runJob(conf);
    }

    private Path getOutputPath() {
	return new Path(putter.getPathWhereMyFilesAreStored(), "output");
    }

    public static class Map extends MapReduceBase implements
	    Mapper<LongWritable, Text, Text, NullWritable> {

	public void map(LongWritable key, Text value,
		OutputCollector<Text, NullWritable> output, Reporter reporter)
		throws IOException {
	    NullWritable nullwr = NullWritable.get();
	    try {
		FEvent event = getEventObject(value.toString());
		value.set(event.getBody());
		output.collect(value, nullwr);
	    } catch (Exception e) {
		throw new IOException(e);
	    }
	}

	private FEvent getEventObject(String item) throws Exception {
	    JsonFactory f = new JsonFactory();
	    JsonParser jp = f.createJsonParser(item);
	    FEvent event = new FEvent();
	    jp.nextToken(); // will return JsonToken.START_OBJECT (verify?)
	    while (jp.nextToken() != JsonToken.END_OBJECT) {
		String fieldname = jp.getCurrentName();
		jp.nextToken(); // move to value, or START_OBJECT/START_ARRAY
		if ("fields".equals(fieldname)) {
		    FEvent.Fields field = new FEvent.Fields();
		    while (jp.nextToken() != JsonToken.END_OBJECT) {
			String namefield = jp.getCurrentName();
			jp.nextToken(); // move to value
			if ("source".equals(namefield)) {
			    field.setSource(jp.getText());
			} else if ("sourceType".equals(namefield)) {
			    field.setSourceType(jp.getText());
			} else {
			    throw new IllegalStateException(
				    "Unrecognized field '" + fieldname + "'!");
			}
		    }
		    event.setFields(field);
		} else if ("body".equals(fieldname)) {
		    event.setBody(jp.getText());
		} else if ("host".equals(fieldname)) {
		    event.setHost(jp.getText());
		} else if ("timestamp".equals(fieldname)) {
		    event.setTimestamp(jp.getText());
		}
	    }
	    return event;
	}
    }

    /**
     * What is FEvent?
     */
    public static class FEvent {
	private String body;
	private String timestamp;
	private String host;
	private Fields fields;

	public Fields getFields() {
	    return this.fields;
	}

	public void setFields(Fields fields) {
	    this.fields = fields;
	}

	public String getBody() {
	    return body;
	}

	public void setBody(String body) {
	    this.body = body;
	}

	public String getTimestamp() {
	    return timestamp;
	}

	public void setTimestamp(String timestamp) {
	    this.timestamp = timestamp;
	}

	public String getHost() {
	    return host;
	}

	public void setHost(String host) {
	    this.host = host;
	}

	public static class Fields {
	    private String sourceType;
	    private String source;

	    public String getSourceType() {
		return sourceType;
	    }

	    public void setSourceType(String sourceType) {
		this.sourceType = sourceType;
	    }

	    public String getSource() {
		return source;
	    }

	    public void setSource(String source) {
		this.source = source;
	    }

	}

    }

}
