package com.splunk.shep.s2s.forwarder;


import static org.testng.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

//comment this test due to this: https://issues.apache.org/jira/browse/HBASE-4709
//@Test(groups = { "slow-unit" })
public class TestHDFSSink {
    private static Log LOG = LogFactory.getLog(TestHDFSSink.class);
    private static MiniDFSCluster dfsCluster;
    private static HDFSSink sink;
    private static String filePath = "splunkeventdata";

    @BeforeClass
    public static void setUp() throws Exception {
	sink = new HDFSSink();
	Configuration conf = new Configuration();
	// int port = Integer.parseInt(sink.getPort());
	// conf.set("fs.default.name",
	// String.format("hdfs://%s:%d", sink.getIp(), port));
	dfsCluster = new MiniDFSCluster(conf, 2, true, null);
	// dfsCluster = new MiniDFSCluster(port, conf, 2, true, true, true,
	// null,
	// null, null, null);
	FileSystem fs = dfsCluster.getFileSystem();
	sink.setConf(conf);
	sink.setFileSystem(fs);
	sink.setIp("127.0.0.1");
	sink.setPort("" + dfsCluster.getNameNodePort());
	sink.start(filePath);
	sink.init();
    }

    @AfterClass
    public static void tearDown() {
	if (dfsCluster != null) {
	    dfsCluster.shutdown();
	}
    }

    @Test
    public void testHDFSSink() throws Exception {
	String data1 = "2011-09-19 17:04:11 this is line1";
	String sourceType = "mySourceType";
	String source = "mySource";
	String host = "localhost";
	long time1 = System.currentTimeMillis();
	sink.write(data1, sourceType, source, host, time1);
	String data2 = "2011-09-19 17:04:12 this is line2";
	long time2 = System.currentTimeMillis() + 1000;
	sink.write(data2, sourceType, source, host, time2);
	sink.close();

	verifyLine(data1, time1, host, sourceType, source);
	verifyLine(data2, time2, host, sourceType, source);
	// FIXME readLine not work for second line
	// LOG.info("line1: " + sink.readLine());
	// LOG.info("line2: " + sink.readLine());
    }

    private void verifyLine(String data, long time, String host,
	    String sourceType, String source) {
	String actualLine = sink.read().trim();
	LOG.debug("actualLine:[" + actualLine + "]");
	LOG.debug("actualLine length:" + actualLine.length());
	String expectedLine = String
		.format("{\"body\":\"%s\",\"timestamp\":\"%d\",\"host\":\"%s\",\"fields\":{\"sourceType\":\"%s\",\"source\":\"%s\"}}",
			data, time, host, sourceType, source);
	LOG.debug("expectedLine:[" + expectedLine + "]");
	LOG.debug("expectedLine length:" + expectedLine.length());
	assertEquals(expectedLine, actualLine);
    }

}
