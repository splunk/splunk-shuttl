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
import java.util.Collections;
import java.util.Map;

import org.apache.log4j.Logger;

import com.splunk.Index;
import com.splunk.Service;
import com.splunk.shuttl.archiver.model.IllegalIndexException;

/**
 * Gets settings from the configured Splunk.
 */
public class SplunkIndexesLayer {

	private final Service splunkService;
	private static final Logger logger = Logger
			.getLogger(SplunkIndexesLayer.class);

	/**
	 * @param splunkService
	 */
	public SplunkIndexesLayer(Service splunkService) {
		this.splunkService = splunkService;
	}

	/**
	 * @return index name mapped to a Splunk index.
	 */
	public Map<String, Index> getIndexes() {
		Map<String, Index> indexes = splunkService.getIndexes();
		if (indexes == null || indexes.isEmpty()) {
			return Collections.emptyMap();
		} else
			return indexes;
	}

	/**
	 * @return thaw location for specified index.
	 * @throws IllegalIndexException
	 *           if index does not exist in splunk
	 */
	public File getThawLocation(String index) throws IllegalIndexException {
		Index splunkIndex = getIndexChecked(index);
		return new File(splunkIndex.getThawedPathExpanded());
	}

	private Index getIndexChecked(String index) {
		Index splunkIndex = getIndexes().get(index);
		if (splunkIndex == null)
			throwAndLogNonExistingSplunkIndex(index);
		return splunkIndex;
	}

	private void throwAndLogNonExistingSplunkIndex(String index)
			throws IllegalIndexException {
		logger.error(did("Attempted to get thaw location for index",
				"index not in splunk", null, "index", index, "splunk service",
				splunkService.getHost()));
		throw new IllegalIndexException("Index " + index
				+ " does not exist in splunk");
	}

	/**
	 * @return colddb location for specified index
	 */
	public File getColdLocation(String index) {
		return new File(getIndexChecked(index).getColdPathExpanded());
	}
}
