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

import java.util.Arrays;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.ArchiveConfiguration;
import com.splunk.shep.archiver.archive.BucketFormat;

@Test(groups = { "fast-unit" })
public class BucketFormatChooserPrioritizedTest {

    BucketFormatChooser bucketFormatChooser;
    ArchiveConfiguration configuration;

    @BeforeMethod
    public void setUp() {
	configuration = mock(ArchiveConfiguration.class);
	bucketFormatChooser = new BucketFormatChooser(configuration);

	stubPrioritizedBucketFormatsAsUnknownThenSplunkBucket();
    }

    private void stubPrioritizedBucketFormatsAsUnknownThenSplunkBucket() {
	when(configuration.getBucketFormatPriority())
		.thenReturn(
			Arrays.asList(BucketFormat.UNKNOWN,
				BucketFormat.SPLUNK_BUCKET));
    }

    @Test(groups = { "fast-unit" })
    public void chooseBucketFormat_givenUnknownFormatIsMostPrioritized_chooseUnknownFormatOutOfSplunkAndUnknown() {
	List<BucketFormat> formats = Arrays.asList(BucketFormat.SPLUNK_BUCKET,
		BucketFormat.UNKNOWN);

	BucketFormat chosenFormat = bucketFormatChooser
		.chooseBucketFormat(formats);
	assertEquals(chosenFormat, BucketFormat.UNKNOWN);
    }

    public void chooseBucketFormat_givenUnknownFormatNotInAvailableFormats_returnSecondPriorityFormat() {
	List<BucketFormat> secondPriorityFormat = Arrays.asList(
		BucketFormat.SPLUNK_BUCKET, BucketFormat.SPLUNK_BUCKET);
	assertEquals(2, secondPriorityFormat.size());

	BucketFormat chosenFormat = bucketFormatChooser
		.chooseBucketFormat(secondPriorityFormat);
	assertEquals(chosenFormat, BucketFormat.SPLUNK_BUCKET);
    }

}
