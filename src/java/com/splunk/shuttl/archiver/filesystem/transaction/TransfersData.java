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
package com.splunk.shuttl.archiver.filesystem.transaction;

import java.io.File;
import java.io.IOException;

/**
 * Transferring data as a {@link AbstractTransaction}. <br/>
 * When transferring data as a {@link AbstractTransaction}, we want to transfer the data
 * to a temporary location and move the data to the real destination, once the
 * transfer has completed.
 */
public interface TransfersData<T> {

	/**
	 * @param localBucket
	 *          - The data to be transferred.
	 * @param temp
	 *          - Path to the temporary transfer location.
	 * @param dst
	 *          - Path to the final destination.
	 * @throws IOException
	 */
	void put(T localData, String temp, String dst) throws IOException;

	/**
	 * @param remoteBucket
	 *          - The remote data to get.
	 * @param temp
	 *          - The temporary local transfer location.
	 * @param dst
	 *          - The final local destination.
	 * @throws IOException
	 */
	void get(T remoteData, File temp, File dst) throws IOException;

}
