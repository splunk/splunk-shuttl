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

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketDeleterTest {

    private Bucket bucket;
    private BucketDeleter bucketDeleter;
    private Logger logger;

    @BeforeMethod
    public void setUp() {
	bucket = UtilsBucket.createTestBucket();
	logger = mock(Logger.class);
	bucketDeleter = new BucketDeleter(logger);
    }

    public void deleteBucket_givenExistingBucket_deletesBucket_deleteTwiceDoesNothing() {
	assertTrue(bucket.getDirectory().exists());
	bucketDeleter.deleteBucket(bucket);
	assertFalse(bucket.getDirectory().exists());
	bucketDeleter.deleteBucket(bucket);
    }

    public void deleteBucket_deletionThrowsException_logsException()
	    throws IOException {
	Bucket throwsIOExceptionOnDelete = mock(Bucket.class);
	doThrow(IOException.class).when(throwsIOExceptionOnDelete)
		.deleteBucket();
	bucketDeleter.deleteBucket(throwsIOExceptionOnDelete);
	verify(logger).warn(anyString());
    }
}
