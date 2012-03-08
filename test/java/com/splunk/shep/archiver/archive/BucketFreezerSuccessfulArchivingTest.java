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
package com.splunk.shep.archiver.archive;

import static com.splunk.shep.testutil.UtilsFile.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.splunk.shep.testutil.UtilsMockito;

/**
 * Fixture: HttpClient returns HttpStatus codes that represent successful
 * archiving.
 */
@Test(groups = { "fast" })
public class BucketFreezerSuccessfulArchivingTest {

    BucketFreezer bucketFreezer;

    @BeforeClass(groups = { "fast" })
    public void beforeClass() throws ClientProtocolException, IOException {
	HttpClient okReturningHttpClient = UtilsMockito
		.createAlwaysOKReturningHTTPClientMock();
	bucketFreezer = new BucketFreezer(getSafeLocationPath(),
		okReturningHttpClient, null);
    }

    @AfterMethod(groups = { "fast" })
    public void tearDownFast() {
	FileUtils.deleteQuietly(getSafeLocationDirectory());
    }

    /**
     * This location is torn down by the AfterMethod annotation.
     */
    private String getSafeLocationPath() {
	return FileUtils.getUserDirectoryPath() + "/" + getClass().getName();
    }

    private File getSafeLocationDirectory() {
	return new File(getSafeLocationPath());
    }

    public void createWithDeafultSafeLocationAndHTTPClient_initialize_nonNullValue() {
	assertNotNull(BucketFreezer
		.createWithDeafultSafeLocationAndHTTPClient());
    }

    public void should_moveDirectoryToaSafeLocation_when_givenPath()
	    throws IOException {
	File safeLocationDirectory = getSafeLocationDirectory();
	assertTrue(safeLocationDirectory.mkdirs());
	File dirToBeMoved = createTempDirectory();

	// Test
	int exitStatus = bucketFreezer.freezeBucket("index-name",
		dirToBeMoved.getAbsolutePath());
	assertEquals(0, exitStatus);

	// Verify
	assertTrue(!dirToBeMoved.exists());
	assertTrue(safeLocationDirectory.exists());
	assertTrue(!isDirectoryEmpty(safeLocationDirectory));
    }

    public void freezeBucket_givenNonExistingSafeLocation_createSafeLocation()
	    throws IOException {
	File nonExistingSafeLocation = getSafeLocationDirectory();
	assertTrue(!nonExistingSafeLocation.exists());
	File dirToBeMoved = createTempDirectory();

	// Test
	bucketFreezer.freezeBucket("index", dirToBeMoved.getAbsolutePath());

	// Verify
	assertTrue(!dirToBeMoved.exists());
	assertTrue(nonExistingSafeLocation.exists());
    }
}
