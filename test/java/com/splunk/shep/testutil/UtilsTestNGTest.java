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
package com.splunk.shep.testutil;

import static org.testng.AssertJUnit.*;

import java.io.IOException;
import java.net.URI;

import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.BucketFormat;
import com.splunk.shep.archiver.model.Bucket;

@Test(groups = { "fast" })
public class UtilsTestNGTest {

    URI uri = URI.create("remote:/valid/uri");
    String index = "index";
    String name = "name";
    BucketFormat format = BucketFormat.UNKNOWN;
    Bucket bucket1;
    Bucket bucket2;

    private void isBucketEqualTest() {
	UtilsTestNG.assertBucketsGotSameIndexFormatAndName(bucket1, bucket2);
	assertTrue(UtilsTestNG.isBucketEqualOnIndexFormatAndName(bucket1,
		bucket2));
    }

    public void isBucketEqualOnIndexFormatAndName_equallyCreatedBuckets_true()
	    throws IOException {
	bucket1 = new Bucket(uri, index, name, format);
	bucket2 = new Bucket(uri, index, name, format);
	isBucketEqualTest();
    }

    public void isBucketEqualOnIndexFormatAndName_indexNull_true()
	    throws IOException {
	bucket1 = new Bucket(uri, null, name, format);
	bucket2 = new Bucket(uri, null, name, format);
	isBucketEqualTest();
    }

    public void isBucketEqualOnIndexFormatAndName_nameNull_true()
	    throws IOException {
	bucket1 = new Bucket(uri, index, null, format);
	bucket2 = new Bucket(uri, index, null, format);
	isBucketEqualTest();
    }

    public void isBucketEqualOnIndexFormatAndName_formatNull_true()
	    throws IOException {
	bucket1 = new Bucket(uri, index, name, null);
	bucket2 = new Bucket(uri, index, name, null);
	isBucketEqualTest();
    }

    private void isBucketDifferentTest() {
	assertFalse(UtilsTestNG.isBucketEqualOnIndexFormatAndName(bucket1,
		bucket2));
    }

    public void isBucketEqualOnIndexFormatAndName_oneIndexNull_false()
	    throws IOException {
	bucket1 = new Bucket(uri, null, name, format);
	bucket2 = new Bucket(uri, index, name, format);
	isBucketDifferentTest();
	bucket1 = new Bucket(uri, index, name, format);
	bucket2 = new Bucket(uri, null, name, format);
	isBucketDifferentTest();
    }

    public void isBucketEqualOnIndexFormatAndName_oneNameNull_false()
	    throws IOException {
	bucket1 = new Bucket(uri, index, null, format);
	bucket2 = new Bucket(uri, index, name, format);
	isBucketDifferentTest();
	bucket1 = new Bucket(uri, index, name, format);
	bucket2 = new Bucket(uri, index, null, format);
	isBucketDifferentTest();
    }

    public void isBucketEqualOnIndexFormatAndName_oneFormatNull_false()
	    throws IOException {
	bucket1 = new Bucket(uri, index, name, null);
	bucket2 = new Bucket(uri, index, name, format);
	isBucketDifferentTest();
	bucket1 = new Bucket(uri, index, name, format);
	bucket2 = new Bucket(uri, index, name, null);
	isBucketDifferentTest();
    }

}
