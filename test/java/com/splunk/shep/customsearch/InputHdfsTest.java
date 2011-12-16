package com.splunk.shep.customsearch;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class InputHdfsTest extends SplunkHdfsTest {
    String testuri = null;

    String line1 = "this is line1";
    String line2 = "this is line2";

    @Parameters({ "username", "password", "splunk.home" })
    @Test(groups = { "fast" })
    public void fileCheck(String username, String password, String splunkhome) {
	System.out.println("Running InputHdfs Test");
	try {
	    Runtime rt = Runtime.getRuntime();
	    String cmdarray[] = { splunkhome + "/bin/splunk", "search",
		    "| inputhdfs file=" + testuri, "-auth",
		    username + ":" + password };
	    Process proc = rt.exec(cmdarray);
	    InputStream stdin = proc.getInputStream();
	    InputStreamReader isr = new InputStreamReader(stdin);
	    BufferedReader br = new BufferedReader(isr);
	    String readline1 = br.readLine();
	    String readline2 = br.readLine();
	    if (!readline2.endsWith(line2)) {
		Assert.fail("Data incorrect in file - " + line2);
	    }
	} catch (Throwable t) {
	    t.printStackTrace();
	    Assert.fail(t.getMessage());
	}
    }

    @Parameters({ "inputhdfstesturi" })
    @BeforeTest(groups = { "fast" })
    public void beforeTest(String uri) {
	this.testuri = uri;
	StringBuffer msg = new StringBuffer();
	msg.append(line1);
	msg.append("\n");
	msg.append(line2);
	msg.append("\n");
	try {
	    putFileinHDFS(this.testuri, msg.toString());
	} catch (Exception e) {
	    e.printStackTrace();
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
