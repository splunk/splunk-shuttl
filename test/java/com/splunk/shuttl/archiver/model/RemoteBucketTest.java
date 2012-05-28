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
package com.splunk.shuttl.archiver.model;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.AssertJUnit.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.FileNotDirectoryException;
import com.splunk.shuttl.archiver.model.RemoteBucketException;

@Test(groups = { "fast-unit" })
public class RemoteBucketTest {

    Bucket bucket;
    URI uri;
    String bucketName;
    String index;
    BucketFormat format;

    @BeforeMethod
    public void setUp() throws IOException {
	uri = URI.create("valid:/uri");
	bucketName = "bucketName";
	index = "index";
	format = BucketFormat.UNKNOWN;
	bucket = new Bucket(uri, index, bucketName, format);
    }

    @Test(groups = { "fast-unit" })
    public void uriConstructor_nonFileUri_ok() throws IOException {
	new Bucket(uri, null, null, null);
    }

    public void getURI_initWithUri_uri() {
	assertEquals(uri, bucket.getURI());
    }

    public void getName_initWithBucketName_bucketName() {
	assertEquals(bucketName, bucket.getName());
    }

    public void getIndex_initWithIndexName_indexName() {
	assertEquals(index, bucket.getIndex());
    }

    public void getFormat_initWithFormat_format() {
	assertEquals(format, bucket.getFormat());
    }

    @Test(expectedExceptions = { RemoteBucketException.class })
    public void getDirectory_initWithNonFileUri_throwsRemoteBucketException() {
	bucket.getDirectory();
    }

    @Test(expectedExceptions = { RemoteBucketException.class })
    public void moveBucketToDir_initWithNonFileUri_throwsRemoteBucketException()
	    throws FileNotFoundException, FileNotDirectoryException {
	bucket.moveBucketToDir(createTempDirectory());
    }

    @Test(expectedExceptions = { RemoteBucketException.class })
    public void deleteBucket_initWithNonFileUri_throwsRemoteBucketException()
	    throws IOException {
	bucket.deleteBucket();
    }

    @Test
    public void isRemote_afterSetUp_true() {
	assertTrue(bucket.isRemote());
    }

}
