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

import static org.testng.AssertJUnit.*;

import java.io.IOException;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.BucketFormat;

@Test(groups = { "fast-unit" })
public class RemoteBucketTest {

	Bucket bucket;
	String path;
	String bucketName;
	String index;
	BucketFormat format;

	@BeforeMethod
	public void setUp() throws IOException {
		path = "/valid/remote/bucket/path";
		bucketName = "bucketName";
		index = "index";
		format = BucketFormat.UNKNOWN;
		bucket = new RemoteBucket(path, index, bucketName, format);
	}

	@Test(groups = { "fast-unit" })
	public void uriConstructor_nonFileUri_ok() throws IOException {
		new Bucket(path, null, null, null);
	}

	public void getPath_initWithUri_uri() {
		assertEquals(path, bucket.getPath());
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
}
