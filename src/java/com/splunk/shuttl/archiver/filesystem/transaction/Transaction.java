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

/**
 * Transfer data to a file system as a transaction.
 */
public interface Transaction {

	/**
	 * Transfer the data invisible from the rest of the system. This must be
	 * blocking until the whole preparation is done.
	 */
	void prepare();

	/**
	 * Make an atomic operation that commits the data to the system. This will
	 * happen when prepare is done.
	 */
	void commit();

	/**
	 * Clean any data that litters the system. Will be called whatever happens in
	 * the prepare or commit stages.
	 */
	void clean();
}
