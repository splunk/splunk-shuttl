// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// License); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.splunk.shuttl.testutil;

/**
 * MethodCallerHelper is a class for finding out who outside of your own class,
 * called you. <br>
 * See the test in {@link MethodCallerHelperTest} to get an understanding for
 * the class.
 * 
 * @author periksson@splunk.com
 * 
 */
public class MethodCallerHelper {

    protected static final int INDEX_OF_CALLER_TO_THIS_METHOD = 3;

    public static Class<?> getCallerToMyMethod() {
	StackTraceElement[] elements = Thread.getAllStackTraces().get(
		Thread.currentThread());
	return getCallerToMyMethod(elements);
    }

    protected static Class<?> getCallerToMyMethod(StackTraceElement[] elements) {
	StackTraceElement caller = elements[INDEX_OF_CALLER_TO_THIS_METHOD];
	StackTraceElement callersCaller = getCallersCaller(elements, caller);
	return getClass(callersCaller);
    }

    private static StackTraceElement getCallersCaller(
	    StackTraceElement[] elements, StackTraceElement caller) {
	for (int i = INDEX_OF_CALLER_TO_THIS_METHOD; i < elements.length; i++)
	    if (!isSameClassAsCaller(caller, elements[i]))
		return elements[i];
	throw new IllegalStateException(
		"There should always be a class that called the caller. "
			+ "Otherwise this code would be run by the Thread class directly, "
			+ "which would be wierd.");
    }

    private static boolean isSameClassAsCaller(StackTraceElement caller,
	    StackTraceElement element) {
	return element.getClassName().equals(caller.getClassName());
    }

    private static Class<?> getClass(StackTraceElement callersCaller) {
	try {
	    return Class.forName(callersCaller.getClassName());
	} catch (ClassNotFoundException e) {
	    throw new RuntimeException(e);
	}
    }
}
