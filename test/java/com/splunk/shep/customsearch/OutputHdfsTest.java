package com.splunk.shep.customsearch;

import org.apache.hadoop.fs.FSDataInputStream;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class OutputHdfsTest extends SplunkHdfsTest {

    String testuri = "hdfs://localhost:54310/outputhdfstest/testfile";

    @Test(groups = { "fast" })
    public void fileCheck() {

	System.out.println("Running OutputHdfs Test");
	try {
	    Runtime rt = Runtime.getRuntime();
	    String cmdarray[] = { "/Applications/splunk/bin/splunk", "search",
		    "* | head 1 | outputhdfs file=" + testuri };
	    Process proc = rt.exec(cmdarray);
	    Thread.sleep(2000);
	    FSDataInputStream is = getFileinHDFS(testuri);
	    if (is == null) {
		Assert.fail("File not created by outputhdfs in HDFS");
	    }

	} catch (Throwable t) {
	    t.printStackTrace();
	    Assert.fail(t.getMessage());
	}
    }

    @BeforeTest(groups = { "fast" })
    public void beforeTest() {
	try {
	    // TODO mkdirsinHDFS(testuri);
	} catch (Exception e) {
	    Assert.fail(e.getMessage());
	}
    }

    @AfterTest(groups = { "fast" })
    public void afterTest() {
	try {
	    deleteFileinHDFS(testuri);
	} catch (Exception e) {
	    Assert.fail(e.getMessage());
	}
    }

}
