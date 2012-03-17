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

import static org.mockito.Mockito.*;

import org.testng.annotations.Test;

import com.splunk.shep.archiver.listers.ArchiveBucketsLister;

@Test(groups = { "fast" })
public class BucketThawerTest {

    BucketThawer bucketThawer;
    ArchiveBucketsLister archiveBucketsLister;


    public void thawBuckets_givenIndex_listBucketsWithinThatIndex() {
	archiveBucketsLister = mock(ArchiveBucketsLister.class);
	bucketThawer = new BucketThawer(archiveBucketsLister);
	String index = "index";
	bucketThawer.thawBuckets(index);
	verify(archiveBucketsLister).listBucketsInIndex(index);
    }

}
