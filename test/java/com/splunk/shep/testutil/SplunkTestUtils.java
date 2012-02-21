package com.splunk.shep.testutil;

import com.splunk.Job;

/**
 * Util class for tests that are run against Splunk.
 */
public class SplunkTestUtils {

    private static final int SLEEP_IN_MILLIS_BETWEEN_JOB_REFRESH = 30;

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

}
