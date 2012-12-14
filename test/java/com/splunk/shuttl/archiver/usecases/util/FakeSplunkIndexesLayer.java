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
package com.splunk.shuttl.archiver.usecases.util;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.util.Map;

import com.splunk.Index;
import com.splunk.shuttl.archiver.model.IllegalIndexException;
import com.splunk.shuttl.archiver.thaw.SplunkIndexesLayer;

/**
 * @author petterik
 * 
 */
public class FakeSplunkIndexesLayer extends SplunkIndexesLayer {

	private File thawLocation;

	/**
	 * @param thawLocation
	 */
	public FakeSplunkIndexesLayer(File thawLocation) {
		super(null);
		this.thawLocation = thawLocation;
	}

	@Override
	public File getThawLocation(String index) throws IllegalIndexException {
		return thawLocation;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Index> getIndexes() {
		Map<String, Index> indexes = mock(Map.class);
		setupThawPathForAllIndexes(indexes);
		return indexes;
	}

	private void setupThawPathForAllIndexes(Map<String, Index> indexes) {
		Index anyIndex = mock(Index.class);
		when(indexes.get(anyString())).thenReturn(anyIndex);
		when(anyIndex.getThawedPathExpanded()).thenReturn(
				thawLocation.getAbsolutePath());
	}
}
