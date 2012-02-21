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
package com.splunk.shep.s2s;

/**
 * @author kpakkirisamy
 *
 */
public class S2SException extends Exception {

    /**
     * 
     */
    public S2SException() {
	super();
	// TODO Auto-generated constructor stub
    }

    /**
     * @param message
     * @param cause
     */
    public S2SException(String message, Throwable cause) {
	super(message, cause);
	// TODO Auto-generated constructor stub
    }

    /**
     * @param message
     */
    public S2SException(String message) {
	super(message);
	// TODO Auto-generated constructor stub
    }

    /**
     * @param cause
     */
    public S2SException(Throwable cause) {
	super(cause);
	// TODO Auto-generated constructor stub
    }

}
