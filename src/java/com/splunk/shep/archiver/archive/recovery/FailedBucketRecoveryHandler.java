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
package com.splunk.shep.archiver.archive.recovery;

import com.splunk.shep.archiver.model.Bucket;

/**
 * Handle the recovery of a failed bucket.
 */
public interface FailedBucketRecoveryHandler {

    /**
     * Recover the failure of a bucket, by trying to archive it again.
     * 
     * @param failedBucket
     *            that failed to be successfully archived.
     */
    void recoverFailedBucket(Bucket failedBucket);
}
