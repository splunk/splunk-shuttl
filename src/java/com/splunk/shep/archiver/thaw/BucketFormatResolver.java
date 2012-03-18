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

import java.util.List;

import com.splunk.shep.archiver.archive.BucketFormat;
import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shep.archiver.model.Bucket;

/**
 * Resolves the format of a {@link Bucket} that is stored on a
 * {@link ArchiveFileSystem}.
 */
public class BucketFormatResolver {

    /**
     * @param buckets
     *            without {@link BucketFormat} set.
     * @return buckets with {@link BucketFormat} set.
     */
    public List<Bucket> resolveBucketsFormats(List<Bucket> buckets) {
	throw new UnsupportedOperationException();
    }

}
