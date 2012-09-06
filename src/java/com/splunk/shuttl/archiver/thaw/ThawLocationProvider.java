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

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Provides location for where to thaw {@link Bucket}s.
 */
public class ThawLocationProvider {

	private final SplunkSettings splunkSettings;
	private final File transferLocation;

	/**
	 * @param splunkSettings
	 *          for looking up the thaw directory.
	 * @param transferLocation
	 *          thaw buckets live while they are transfered.
	 */
	public ThawLocationProvider(SplunkSettings splunkSettings,
			File transferLocation) {
		this.splunkSettings = splunkSettings;
		this.transferLocation = transferLocation;
	}

	/**
	 * @param bucket
	 *          to get location in thaw for.
	 * @throws IOException
	 *           if that location can not be found.
	 */
	public File getLocationInThawForBucket(Bucket bucket) throws IOException {
		File thawLocation = splunkSettings.getThawLocation(bucket.getIndex());
		return new File(thawLocation, bucket.getName());
	}

	/**
	 * @param bucket
	 *          to get transfer location for.
	 * @return non existing local where the bucket can be transfered.
	 */
	public File getThawTransferLocation(Bucket bucket) {
		File file = new File(transferLocation, bucket.getName());
		if (file.exists())
			deleteFile(file);
		return file;
	}

	private void deleteFile(File file) {
		boolean wasDeleted = file.delete();
		if (!wasDeleted)
			Logger.getLogger(getClass()).warn(
					warn("Tried deleting a file", "Could not delete it",
							"Will not do anything", "file", file));
	}

}
