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
package com.splunk.shep.archiver.thaw;

import static com.splunk.shep.testutil.UtilsFile.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.File;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.SplunkSettings;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsBucket;

@Test(groups = { "fast" })
public class ThawLocationProviderTest {

    ThawLocationProvider thawLocationProvider;
    SplunkSettings splunkSettings;
    Bucket bucket;
    File thawLocation;

    @BeforeMethod
    public void setUp() {
	bucket = UtilsBucket.createTestBucket();
	splunkSettings = mock(SplunkSettings.class);
	thawLocationProvider = new ThawLocationProvider(splunkSettings);

	thawLocation = createTestFilePath();
	when(splunkSettings.getThawLocation(bucket.getIndex())).thenReturn(
		thawLocation);
    }

    @Test(groups = { "fast" })
    public void getLocationInThawForBucket_givenThawLocation_returnedBucketDirectorysParentIsThawLocation() {
	File locationInThawDirectory = thawLocationProvider
		.getLocationInThawForBucket(bucket);
	assertEquals(thawLocation.getAbsolutePath(), locationInThawDirectory
		.getParentFile().getAbsolutePath());
    }

    public void getLocationInThawForBucket_givenThawLocation_directoryHasNameOfBucket() {
	File bucketsLocation = thawLocationProvider
		.getLocationInThawForBucket(bucket);
	assertEquals(bucket.getName(), bucketsLocation.getName());
    }
}
