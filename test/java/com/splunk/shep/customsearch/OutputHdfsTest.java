package com.splunk.shep.customsearch;

import org.apache.hadoop.fs.FSDataInputStream;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class OutputHdfsTest extends SplunkHdfsTest {

    String testuri = null;

    @Parameters({ "username", "password" })
    @Test(groups = { "fast" })
    public void fileCheck(String username, String password) {
	System.out.println("Running OutputHdfs Test");
	try {
	    Runtime rt = Runtime.getRuntime();
	    String cmdarray[] = { "/Applications/splunk/bin/splunk", "search",
		    "* | head 1 | outputhdfs file=" + testuri, "-auth",
		    username + ":" + password };
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

    @Parameters({ "outputhdfstesturi" })
    @BeforeTest(groups = { "fast" })
    public void beforeTest(String uri) {
	this.testuri = uri;
	try {
	    // TODO mkdirsinHDFS(this.testuri);
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
