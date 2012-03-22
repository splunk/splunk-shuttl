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
import static org.testng.AssertJUnit.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.ArchiveConfiguration;
import com.splunk.shep.archiver.archive.BucketFormat;

@Test(groups = { "fast-unit" })
public class BucketFormatChooserTest {

    BucketFormatChooser bucketFormatChooser;
    ArchiveConfiguration configuration;

    @BeforeMethod
    public void setUp() {
	configuration = mock(ArchiveConfiguration.class);
	bucketFormatChooser = new BucketFormatChooser(configuration);
    }

    @Test(groups = { "fast-unit" })
    public void chooseBucketFormat_givenEmptyList_unknown() {
	List<BucketFormat> emptyList = Arrays.asList();
	assertTrue(emptyList.isEmpty());
	BucketFormat actualFormat = bucketFormatChooser
		.chooseBucketFormat(emptyList);
	assertEquals(BucketFormat.UNKNOWN, actualFormat);
    }

    public void chooseBucketFormat_givenListWithOneBucketFormat_returnThatFormat() {
	BucketFormat format = BucketFormat.SPLUNK_BUCKET;
	List<BucketFormat> singleBucketFormat = Arrays.asList(format);
	assertEquals(1, singleBucketFormat.size());

	BucketFormat chosenFormat = bucketFormatChooser
		.chooseBucketFormat(singleBucketFormat);
	assertEquals(format, chosenFormat);
    }

    public void chooseBucketFormat_givenTwoFormats_askConfigurationForFormatPriority() {
	List<BucketFormat> twoBuckets = Arrays.asList(
		BucketFormat.SPLUNK_BUCKET, BucketFormat.UNKNOWN);
	assertEquals(2, twoBuckets.size());

	bucketFormatChooser.chooseBucketFormat(twoBuckets);
	verify(configuration).getBucketFormatPriority();
    }

    public void chooseBucketFormat_givenNoPrioritizedFormats_chooseDefaultFormatWhenNoPrioritizing() {
	List<BucketFormat> prioritizedFormats = new ArrayList<BucketFormat>();
	assertTrue(prioritizedFormats.isEmpty());
	when(configuration.getBucketFormatPriority()).thenReturn(
		prioritizedFormats);
	List<BucketFormat> formats = Arrays.asList(
		BucketFormatChooser.DEFAULT_FORMAT_WHEN_NO_PRIORITIZING,
		BucketFormat.UNKNOWN);

	BucketFormat chosenFormat = bucketFormatChooser
		.chooseBucketFormat(formats);
	assertEquals(BucketFormatChooser.DEFAULT_FORMAT_WHEN_NO_PRIORITIZING,
		chosenFormat);
    }

    public void chooseBucketFormat_givenNoPrioritizedFormatsAndDefaultWhenNoPrioritizingDoesntExist_chooseFirstFormat() {
	List<BucketFormat> prioritizedFormats = new ArrayList<BucketFormat>();
	assertTrue(prioritizedFormats.isEmpty());
	when(configuration.getBucketFormatPriority()).thenReturn(
		prioritizedFormats);
	BucketFormat firstFormat = BucketFormat.UNKNOWN;
	List<BucketFormat> formats = Arrays.asList(firstFormat,
		BucketFormat.UNKNOWN);
	assertFalse(formats
		.contains(BucketFormatChooser.DEFAULT_FORMAT_WHEN_NO_PRIORITIZING));

	BucketFormat chosenFormat = bucketFormatChooser
		.chooseBucketFormat(formats);
	assertEquals(BucketFormat.UNKNOWN, chosenFormat);
    }

}
