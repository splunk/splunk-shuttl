// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.splunk.shep.forwarding;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.Args;
import com.splunk.EntityCollection;
import com.splunk.Index;
import com.splunk.InputCollection;
import com.splunk.InputKind;
import com.splunk.Job;
import com.splunk.SavedSearch;
import com.splunk.SavedSearchCollection;
import com.splunk.Service;
import com.splunk.shep.testutil.FileSystemUtils;
import com.splunk.shep.testutil.SplunkServiceParameters;
import com.splunk.shep.testutil.SplunkTestUtils;


public class DirectForwardingTest {
    private static final String TEST_INPUT_FILENAME = "smallsyslog.log";
    private FileSystem fileSystem;
    private Service splunkService;
    private SplunkServiceParameters splunkServiceParams;
    
    private void waitForIndexing(Index index, int value, int seconds) {
        while (seconds > 0) {
            try {
                // 5000ms (5 second sleep)
                Thread.sleep(5000);
                seconds = seconds - 5;
                if (index.getTotalEventCount() == value) {
                    return;
                }
                index.refresh();
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private Index createSplunkIndex(String name) {
	Index index;
	EntityCollection<Index> indexes = splunkService.getIndexes();
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

    @Parameters({ "splunk.host", "splunk.mgmtport", "splunk.username",
	    "splunk.password" })
    @BeforeClass(groups = { "functional", "known-failures" })
    public void setUp(String splunkHost, String splunkMgmtPort,
	    String splunkUser, String splunkPass) throws IOException {
	System.out.println("SetUp for Direct forwarding tests");
	splunkServiceParams = new SplunkServiceParameters(splunkUser,
		splunkPass, splunkHost, splunkMgmtPort, "shep");
	splunkService = splunkServiceParams.getLoggedInService();
	// TODO: set up appending for splunk
    }

    @Test(groups = { "known-failures" })
    public void monitorFileInSplunk() {
	System.out.println("Running monitorFileInSplunk");
        String indexName = "directfwd";
        Index index = createSplunkIndex(indexName);

        InputCollection inputs = splunkService.getInputs();
	File inputFile = new File(SplunkTestUtils.TEST_RESOURCES_PATH
		+ File.separator + TEST_INPUT_FILENAME);
	String name = inputFile.getAbsolutePath();
        Args args = new Args();
        args.put("sourcetype", "syslog");
        args.put("index", indexName);
        inputs.refresh();
        inputs.create(name, InputKind.Monitor, args);
        inputs.refresh();

        // wait at most 1 minute for indexing to complete
        waitForIndexing(index, 100, 60);
	Assert.assertEquals(index.getTotalEventCount(), 100);

        // check that events are searchable
        String query = "search index=directfwd";
        Job job;
        job = splunkService.getJobs().create(query, null);
        SplunkTestUtils.waitWhileJobFinishes(job);
	Assert.assertEquals(job.getEventCount(), 100);
    }

    @Test(groups = { "known-failures" }, dependsOnMethods = { "monitorFileInSplunk" })
    public void checkTotalEventsSearch() throws IOException, InterruptedException {
	// Wait 30 seconds for events to get forwarded
	Thread.sleep(30000);
	
        SavedSearchCollection savedSearches = splunkService.getSavedSearches();
	Assert.assertTrue(savedSearches.containsKey("HC total events"));
        SavedSearch savedSearch = savedSearches.get("HC total events");
        Job job = savedSearch.dispatch();
        SplunkTestUtils.waitWhileJobFinishes(job);

        InputStream is = job.getResults(new Args("output_mode", "json"));
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String,Integer>> results = mapper.readValue(is, 
        	new TypeReference<List<HashMap<String,Integer>>>() { });
	Assert.assertEquals(results.size(), 1, "Search doesn't return 1 result");
	Assert.assertTrue(results.get(0).containsKey("Total Events"),
		"Total Events key doesn't exist in search results");
        int totalEvents = results.get(0).get("Total Events");
	Assert.assertEquals(totalEvents, 100,
		"Failing due to HADOOP-282. HC total events saved search returns incorrect results");
    }

    @Parameters({ "hadoop.host", "hadoop.port" })
    @Test(groups = { "known-failures" }, dependsOnMethods = { "monitorFileInSplunk" })
    public void checkEventCountInHdfs(String hadoopHost, String hadoopPort) 
	    throws IOException, URISyntaxException {
	fileSystem = FileSystemUtils.getRemoteFileSystem(hadoopHost,
		hadoopPort);
	URI hdfsFile = new URI("hdsf", null, hadoopHost,
		Integer.parseInt(hadoopPort), "/splunkeventdata*", null, null);
	Path pattern = new Path(hdfsFile);
	Assert.assertTrue(fileSystem != null, "fileSystem is null");
	FileStatus fs[] = fileSystem.globStatus(pattern);
	Assert.assertTrue(fs.length > 0,
		"No files exist which match the pattern: " + pattern);
	FileStatus latest = fs[0];
	for (int i = 1; i < fs.length; i++) {
	    if (fs[i].getModificationTime() > latest.getModificationTime()) {
		latest = fs[i];
	    }
	}

	FSDataInputStream open = fileSystem.open(latest.getPath());
	List<String> readLines = IOUtils.readLines(open);
	// count the number of events, each event has the word "body" in it
	int numberOfEvents = 0;
	for (String line : readLines) {
	    if (line.contains("body")) {
		numberOfEvents = numberOfEvents + 1;
	    }
	}
	String msg = "HADOOP-271: Incorrect number of events in " + latest.getPath() + ":"
		+ readLines;
	Assert.assertEquals(numberOfEvents, 100, msg);
    }

}
