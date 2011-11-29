// HdfsEvent.java
//
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

package com.splunk.shep.connector;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;

import org.apache.log4j.Logger;

public class HdfsEvent
{
		
	/**
     * Build an HDFS event string.
     *
     * @param rawBytes The byte array
     * @param sourceType The sourceType
	 * @param source The source
	 * @param host The host
	 * @param time The timestamp
     * @return The event string
	 
     */
    public static String build(String data, String sourceType, String source, String host, long time) 
	{
        String event = openField() + 
		                   buildKey("body") + buildValue(data) + "," +
						   buildKey("timestamp") + buildValue(time) + "," +
						   buildKey("host") + buildValue(host) + "," +
						   buildKey("fields") + buildSourceField(sourceType, source) +
		               closeField() + "\n";
        return event;
    }
	
	public static String build(String data, String sourceType, String source, String host, String timestamp) 
	{
        String event = openField() + 
		                   buildKey("body") + buildValue(data) + "," +
						   buildKey("timestamp") + buildValue(timestamp) + "," +
						   buildKey("host") + buildValue(host) + "," +
						   buildKey("fields") + buildSourceField(sourceType, source) +
		               closeField() + "\n";
        return event;
    }
	
	public static String build(String data, String sourceType, String source, String host) 
	{
        String event = openField() + 
		                   buildKey("body") + buildValue(data) + "," +
						   buildKey("timestamp") + buildValue(System.currentTimeMillis()) + "," +
						   buildKey("host") + buildValue(host) + "," +
						   buildKey("fields") + buildSourceField(sourceType, source) +
		               closeField() + "\n";
        return event;
    }
	
	public static String build(byte[] rawBytes, String sourceType, String source, String host, long time) 
	{
        String event = openField() + 
		                   buildKey("body") + buildValue(rawBytes) + "," +
						   buildKey("timestamp") + buildValue(time) + "," +
						   buildKey("host") + buildValue(host) + "," +
						   buildKey("fields") + buildSourceField(sourceType, source) +
		               closeField() + "\n";
        return event;
    }
	
	public static String buildSourceField(String sourceType, String source) 
	{
        String event = openField() + 
		                   buildKey("sourceType") + buildValue(sourceType) + "," +
						   buildKey("source") + buildValue(source) +
		               closeField();
        return event;
    }
	
	public static String buildKey(String key) 
	{
        String keyStr = openValue() + key + closeValue() + ":";
        return keyStr;
    }
	
	public static String buildValue(String value) 
	{
        String valStr = openValue() + value + closeValue();
        return valStr;
    }
	
	public static String buildValue(long value) 
	{
        String valStr = openValue() + value + closeValue();
        return valStr;
    }
	
	public static String buildValue(byte[] bytes) 
	{
        String valStr = openValue() + bytes + closeValue();
        return valStr;
    }
	
	public static String openField() 
	{
        return new String("{");
    }
	
	public static String closeField() 
	{
        return new String("}");
    }
	
	public static String openValue() 
	{
        return new String("\"");
    }
	
	public static String closeValue() 
	{
        return new String("\"");
    }
}