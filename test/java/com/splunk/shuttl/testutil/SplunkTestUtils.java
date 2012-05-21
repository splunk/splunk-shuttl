package com.splunk.shuttl.testutil;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.splunk.EntityCollection;
import com.splunk.Index;
import com.splunk.Job;
import com.splunk.Service;

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

    static public void waitForIndexing(Index index, int value, int seconds) {
	int indexedEventCount = 0;
	while (seconds > 0) {
	    try {
		// 5000ms (5 second sleep)
		Thread.sleep(5000);
		seconds = seconds - 5;
		indexedEventCount = index.getTotalEventCount();
		if (indexedEventCount == value) {
		    return;
		}
		index.refresh();
	    } catch (InterruptedException e) {
		continue;
	    }
	}
	String msg = "Indexing incomplete for index " + index + " after "
		+ seconds + "seconds, expectedCount=" + value + " actualCount="
		+ indexedEventCount;
	throw new RuntimeException(msg);
    }

    public static Index createSplunkIndex(Service service, String name) {
	Index index;
	EntityCollection<Index> indexes = service.getIndexes();
	if (indexes.containsKey(name)) {
	    System.out.println("Index " + name + " already exists");
	    index = indexes.get(name);
	} else {
	    indexes.create(name);
	    index = indexes.get(name);
	    index.refresh();
	    indexes.refresh();
	    System.out.println("Index " + name + " created");
	}

	return index;
    }
}
