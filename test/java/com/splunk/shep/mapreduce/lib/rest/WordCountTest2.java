package com.splunk.shep.mapreduce.lib.rest;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.Job;
import com.splunk.Service;

public class WordCountTest2 {

    @Test(groups = { "slow" })
    @Parameters({ "splunk.host", "splunk.mgmtport", "splunk.username",
	    "splunk.password", "splunk.home" })
    public void wordCountTest2_setUp(String splunkHost, String splunkMGMTPort,
	    String splunkUsername, String splunkPassword, String splunkHome)
	    throws InterruptedException, IOException {

	Service splunk = getLoggedInSplunkService(splunkHost, splunkMGMTPort,
		splunkUsername, splunkPassword);
	if (!isTestFileAlreadyIndexed(splunk))
	    indexTestFile(splunkHome);

    }

    private void indexTestFile(String splunkHome) throws IOException,
	    InterruptedException {
	// There's currently no way to oneshot a file through the Splunk SDK/API
	// yet. Using splunk.home instead.
	File file = new File("test/java/com/splunk/shep/mapreduce/lib/rest"
		+ "/wordfile-timestamp");
	Process exec = Runtime.getRuntime().exec(
		splunkHome + "/bin/splunk add oneshot "
			+ file.getAbsolutePath());
	int exitStatus = exec.waitFor();
	assertEquals(exitStatus, 0);
    }

    private boolean isTestFileAlreadyIndexed(Service splunk)
	    throws InterruptedException {
	Job search = splunk.getJobs().create(
		"search index=main source=*wordfile-timestamp");
	waitWhileSearchFinishes(search);
	return search.getResultCount() > 0;
    }

    private void waitWhileSearchFinishes(Job search)
	    throws InterruptedException {
	while (!search.isDone()) {
	    Thread.sleep(10);
	    search.refresh();
	}
    }

    private Service getLoggedInSplunkService(String splunkHost,
	    String splunkMGMTPort, String splunkUsername, String splunkPassword) {
	Service splunk = new Service(splunkHost,
		Integer.parseInt(splunkMGMTPort));
	splunk.login(splunkUsername, splunkPassword);
	return splunk;
    }

}
