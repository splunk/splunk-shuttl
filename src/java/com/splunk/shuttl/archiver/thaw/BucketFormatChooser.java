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
package com.splunk.shuttl.archiver.thaw;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketFormat;

/**
 * Selects a {@link BucketFormat} given a {@link Set} of {@link BucketFormat}s
 */
public class BucketFormatChooser {

    private final static Logger logger = Logger
	    .getLogger(BucketFormatChooser.class);

    public static final BucketFormat DEFAULT_FORMAT_WHEN_NO_PRIORITIZING = BucketFormat.SPLUNK_BUCKET;

    private final ArchiveConfiguration configuration;

    /**
     * @param configuration
     */
    public BucketFormatChooser(ArchiveConfiguration configuration) {
	this.configuration = configuration;
    }

    /**
     * @param {@link Set} of formats.
     * @return the bucket format of choice. Primarily based on configuration and
     *         then defaults.
     */
    public BucketFormat chooseBucketFormat(List<BucketFormat> formats) {
	if (formats.isEmpty()) {
	    logger.warn(warn("Chose bucket format.",
		    "There were no formats to chose between.",
		    "Chose bucket format UNKNOWN", "chosen_bucket_format",
		    BucketFormat.UNKNOWN));
	    return BucketFormat.UNKNOWN;
	} else if (formats.size() == 1) {
	    return formats.iterator().next();
	} else {
	    return chooseFormatBasedOnPrioritizingOrDefaults(formats);
	}
    }

    private BucketFormat chooseFormatBasedOnPrioritizingOrDefaults(
	    List<BucketFormat> availableFormats) {
	BucketFormat chosenFormat = null;
	if (existsPrioritizedFormats()) {
	    chosenFormat = chooseFromPrioritizedFormat(availableFormats);
	}
	if (chosenFormat == null) {
	    chosenFormat = chooseFromDefaultsAndAvailableFormats(availableFormats);
	}
	return chosenFormat;
    }

    private boolean existsPrioritizedFormats() {
	return !configuration.getBucketFormatPriority().isEmpty();
    }

    private BucketFormat chooseFromPrioritizedFormat(
	    List<BucketFormat> availableFormats) {
	Set<BucketFormat> formatSet = new HashSet<BucketFormat>(
		availableFormats);
	for (BucketFormat prioFormat : configuration.getBucketFormatPriority()) {
	    if (formatSet.contains(prioFormat)) {
		return prioFormat;
	    }
	}
	return null;
    }

    private BucketFormat chooseFromDefaultsAndAvailableFormats(
	    List<BucketFormat> availableFormats) {
	if (availableFormats.contains(DEFAULT_FORMAT_WHEN_NO_PRIORITIZING)) {
	    return DEFAULT_FORMAT_WHEN_NO_PRIORITIZING;
	} else {
	    return availableFormats.get(0);
	}
    }
}
