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
package com.splunk.shep.archiver.functional;

import static com.splunk.shep.testutil.UtilsFile.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.ArchiveConfiguration;
import com.splunk.shep.archiver.archive.BucketArchiver;
import com.splunk.shep.archiver.archive.BucketArchiverFactory;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.archiver.model.FileNotDirectoryException;
import com.splunk.shep.archiver.model.IllegalIndexException;
import com.splunk.shep.archiver.thaw.BucketThawer;
import com.splunk.shep.archiver.thaw.BucketThawer.ThawInfo;
import com.splunk.shep.archiver.thaw.BucketThawerFactory;
import com.splunk.shep.archiver.thaw.SplunkSettings;
import com.splunk.shep.testutil.UtilsBucket;
import com.splunk.shep.testutil.UtilsTestNG;

@Test(enabled = true, groups = { "functional" })
public class ImportCsvFunctionalTest {

    private ArchiveConfiguration localCsvArchiveConfigration;
    private File thawDirectory;
    private BucketThawer csvThawer;
    private Bucket realBucket;
    private BucketArchiver csvArchiver;

    @BeforeMethod
    public void setUp() throws IllegalIndexException {
	localCsvArchiveConfigration = UtilsArchiverFunctional
		.getLocalCsvArchiveConfigration();
	SplunkSettings splunkSettings = mock(SplunkSettings.class);
	thawDirectory = createTempDirectory();
	when(splunkSettings.getThawLocation(anyString())).thenReturn(
		thawDirectory);
	csvThawer = BucketThawerFactory.createWithSplunkSettingsAndConfig(
		splunkSettings, localCsvArchiveConfigration);

	realBucket = UtilsBucket.createRealBucket();
	csvArchiver = BucketArchiverFactory
		.createWithConfiguration(localCsvArchiveConfigration);
    }

    @AfterMethod
    public void tearDown() {
	FileUtils.deleteQuietly(thawDirectory);
    }

    @Parameters(value = { "splunk.home" })
    public void _givenArchivedCsvBucket_thawedBucketEqualsArchivedRealBucket(
	    String splunkHome) throws FileNotFoundException,
	    FileNotDirectoryException {
	archiveBucketAsCsvWithExportToolThatNeedsSplunkHome(splunkHome);
	List<ThawInfo> thawBuckets = csvThawer.thawBuckets(
		realBucket.getIndex(), realBucket.getEarliest(),
		realBucket.getLatest());

	assertEquals(1, thawBuckets.size());
	Bucket thawedBucket = thawBuckets.get(0).bucket;
	UtilsTestNG.assertBucketsGotSameIndexFormatAndName(realBucket,
		thawedBucket);
	assertEquals(sizeOfBucket(realBucket), sizeOfBucket(thawedBucket));
    }

    private void archiveBucketAsCsvWithExportToolThatNeedsSplunkHome(String splunkHome) {
	UtilsArchiverFunctional.archiveBucket(realBucket, csvArchiver,
		splunkHome);
    }

    private long sizeOfBucket(Bucket b) {
	return FileUtils.sizeOfDirectory(b.getDirectory());
    }
}
