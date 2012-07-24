// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// License); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.splunk.shuttl.archiver.archive;

import java.io.File;

import com.splunk.shuttl.archiver.util.UtilsFile;

public enum BucketFormat {
	SPLUNK_BUCKET, UNKNOWN, CSV;

	/**
	 * @param directory
	 *          to a bucket
	 * @return format depending on what is in the bucket directory.
	 */
	public static BucketFormat getFormatFromDirectory(File directory) {
		if (isSplunkBucketFormat(directory))
			return BucketFormat.SPLUNK_BUCKET;
		else if (isCsvBucketFormat(directory))
			return BucketFormat.CSV;
		else
			return BucketFormat.UNKNOWN;
	}

	private static boolean isSplunkBucketFormat(File directory) {
		File rawdataInDirectory = new File(directory, "rawdata");
		return rawdataInDirectory.exists();
	}

	private static boolean isCsvBucketFormat(File directory) {
		File[] filesInDirectory = directory.listFiles();
		if (filesInDirectory != null && filesInDirectory.length == 1)
			return isCsvFile(filesInDirectory[0]);
		return false;
	}

	private static boolean isCsvFile(File file) {
		return UtilsFile.isCsvFile(file);
	}

}
