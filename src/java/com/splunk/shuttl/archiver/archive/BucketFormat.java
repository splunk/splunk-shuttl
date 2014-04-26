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

public enum BucketFormat {
	SPLUNK_BUCKET, UNKNOWN, CSV, SPLUNK_BUCKET_TGZ, CSV_SNAPPY, CSV_BZIP2, CSV_GZIP;

	public static String extensionOfFormat(BucketFormat format) {
		if (format.equals(CSV))
			return ".csv";
		else if (format.equals(SPLUNK_BUCKET_TGZ))
			return ".tgz";
		else if (format.equals(CSV_SNAPPY))
			return ".csv.sz";
		else if (format.equals(CSV_BZIP2))
			return ".csv.bz";
		else if (format.equals(CSV_GZIP))
			return ".csv.gz";
		return "";
	}
}
