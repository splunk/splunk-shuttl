package com.splunk.shep.mapreduce.lib.rest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import com.splunk.shep.mapreduce.lib.rest.tests.WordCount;

public class WordCountTest {

    // Folder where the test input and output will end up.
    private final String TEST_FOLDER_ON_HADOOP = "/"
	    + WordCountTest.class.getSimpleName();
    private final String FILENAME_FOR_FILE_WITH_TEST_INPUT = "file01";
    private final String TEST_FILE_PATH_ON_HADOOP = TEST_FOLDER_ON_HADOOP
	    + FILENAME_FOR_FILE_WITH_TEST_INPUT;

    // @Parameters({ "inputhdfstesturi" })
    // @BeforeTest(groups = { "slow" })
    public void setUp() {
	copyTestFileWithInputToHadoop();
    }

    private void copyTestFileWithInputToHadoop() {
	try {
	    doCopyTestFileWithInputToHadoop();
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }

    private void doCopyTestFileWithInputToHadoop() throws IOException {
	FileSystem fs = FileSystem.get(new Configuration());
	OutputStream outputStream = outputStreamForTestFile(fs);
	InputStream inputStream = getInputStreamForLocalFileWithTestInput();
	copyBytesToFileSystem(fs, outputStream, inputStream);
    }

    private OutputStream outputStreamForTestFile(FileSystem fs)
	    throws IOException {
	return fs.create(new Path(TEST_FILE_PATH_ON_HADOOP));
    }

    private InputStream getInputStreamForLocalFileWithTestInput()
	    throws FileNotFoundException {
	return new FileInputStream(getLocalFileWithTestInput());
    }

    private File getLocalFileWithTestInput() {
	String pathToFileWithTestInput = getClass().getResource(
		FILENAME_FOR_FILE_WITH_TEST_INPUT).getPath();
	return new File(pathToFileWithTestInput);
    }

    private void copyBytesToFileSystem(FileSystem fs,
	    OutputStream outputStream, InputStream inputStream)
	    throws IOException {
	org.apache.hadoop.io.IOUtils.copyBytes(inputStream, outputStream,
		fs.getConf());
    }

    private static class LocalFileToHadoopCopier {

	private final FileSystem fileSystem;
	private final String testFolderOnHadoop;

	public LocalFileToHadoopCopier(FileSystem fileSystem, Class<?> caller) {
	    this.fileSystem = fileSystem;
	    testFolderOnHadoop = "/" + caller.getSimpleName();
	}

	public void copyFileToHadoop(File f) {
	    try {
		doCopyFileToHadoop(f);
	    } catch (IOException e) {
		throw new RuntimeException(e);
	    }
	}

	private void doCopyFileToHadoop(File f) throws IOException {
	    InputStream inputStream = new FileInputStream(f);
	    OutputStream outputStream = outputStreamToHadoopFileSystem();
	    copyBytesToFileSystem(inputStream, outputStream);
	}

	private OutputStream outputStreamToHadoopFileSystem()
		throws IOException {
	    return fileSystem.create(new Path(testFolderOnHadoop));
	}

	private void copyBytesToFileSystem(InputStream inputStream,
		OutputStream outputStream) throws IOException {
	    org.apache.hadoop.io.IOUtils.copyBytes(inputStream, outputStream,
		    fileSystem.getConf());
	}
    }

    // @AfterTest(groups = { "slow" })
    public void tearDown() {

    }

    // @Parameters({ "username", "password", "splunk.home" })
    // @Test(groups = { "slow" })
    public void fileCheck(String username, String password, String splunkhome) {

    }

    public static class Map extends MapReduceBase implements
	    Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(LongWritable key, Text value,
		OutputCollector<Text, IntWritable> output, Reporter reporter)
		throws IOException {
	    String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    while (tokenizer.hasMoreTokens()) {
		word.set(tokenizer.nextToken());
		output.collect(word, one);
	    }
	}
    }

    public static class Reduce extends MapReduceBase implements
	    Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterator<IntWritable> values,
		OutputCollector<Text, IntWritable> output, Reporter reporter)
		throws IOException {
	    int sum = 0;
	    while (values.hasNext()) {
		sum += values.next().get();
	    }
	    output.collect(key, new IntWritable(sum));

	}
    }

    public static void main(String[] args) throws Exception {
	JobConf conf = new JobConf(WordCount.class);
	conf.setJobName("hadoopunittest1");
	SplunkConfiguration.setConnInfo(conf, "localhost", 8089, "admin",
		"changeme");

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);

	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(com.splunk.shep.mapreduce.lib.rest.SplunkOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
    }
}
