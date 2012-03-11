package com.splunk.shep.testutil;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.splunk.Job;

/**
 * Util class for tests that are run against Splunk.
 */
public class SplunkTestUtils {

    private static final int SLEEP_IN_MILLIS_BETWEEN_JOB_REFRESH = 150;

    public static final String TEST_RESOURCES_PATH = "test/resources";

    public static void waitWhileJobFinishes(Job job) {
	while (!job.isDone()) {
	    sleep(SLEEP_IN_MILLIS_BETWEEN_JOB_REFRESH);
	    job.refresh();
	}
    }


    private static void sleep(int millis) {
	try {
	    Thread.sleep(millis);
	} catch (InterruptedException e) {
	    throw new RuntimeException(e);
	}
    }

    public static List<String> readSearchResults(InputStream results) {
        try {
            return IOUtils.readLines(results);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
