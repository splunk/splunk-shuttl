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
package com.splunk.shuttl.server.mbeans;

/**
 * {@link ShuttlArchiver} with path to archiver.xml configuration file.</br>
 * Remove this when we have a better way of configure our MBeans. Meaning when
 * we don't hard code the path to the xml.
 */
// TODO: Read the comment above and remove this class when things are better.
public class ShuttlArchiverForTests extends ShuttlArchiver {

	/**
	 * @throws ShuttlMBeanException
	 */
	public ShuttlArchiverForTests() throws ShuttlMBeanException {
		super();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.splunk.shuttl.server.mbeans.ShuttlArchiver#getArchiverConfXml()
	 */
	@Override
	protected String getArchiverConfXml() {
		return "package/conf/archiver.xml";
	}
}
