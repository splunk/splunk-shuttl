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
package com.splunk;

/**
 * Calls splunk instances shuttl port.
 */
public class ShuttlPortEntity extends Entity {

	public ShuttlPortEntity(Service service) {
		super(service, "/shuttl/port");
	}

	/**
	 * @return the configured shuttl port of the Splunk instance which the service
	 *         is connected to.
	 */
	public int getShuttlPort() {
		throw new UnsupportedOperationException();
	}

}
