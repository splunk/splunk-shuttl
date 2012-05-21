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

import org.apache.log4j.Logger;

import com.splunk.EntityCollection;
import com.splunk.Index;
import com.splunk.Service;
import com.splunk.shuttl.archiver.model.IllegalIndexException;

/**
 * Gets settings from the configured Splunk.
 */
public class SplunkSettings {

    private final Service splunkService;
    private static final Logger logger = Logger.getLogger(SplunkSettings.class);

    /**
     * @param splunkService
     */
    public SplunkSettings(Service splunkService) {
	this.splunkService = splunkService;
    }

    /**
     * @return thaw location for specified index.
     * @throws IllegalIndexException
     *             if index does not exist in splunk
     */
    public File getThawLocation(String index) throws IllegalIndexException {
	EntityCollection<Index> indexes = splunkService.getIndexes();
	Index splunkIndex = indexes.get(index);
	if(splunkIndex == null) {
	    logger.error(did("Attempted to get thaw location for index",
		    "index not in splunk", null, "index", index,
		    "splunk service", splunkService.getHost()));
	    throw new IllegalIndexException("Index " + index
		    + " does not exist in splunk");
	}
	String thawedPathExpanded = splunkIndex.getThawedPathExpanded();
	return new File(thawedPathExpanded);
    }
}
