// EventParser.java
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

public class EventParser {
    private InputStream inStream = null;
    private DataSink emitter = null;

    private String rawValue = null;
    private String rawLength = null;
    // private byte[] rawBuffer = null;
    private String hostValue = null;
    private String sourceValue = null;
    private String sourceTypeValue = null;
    private String timeValue = null;
    private String indexValue = null;

    private boolean hasRaw = false;
    private boolean hasHost = false;
    private boolean hasSource = false;
    private boolean hasSourceType = false;
    private boolean hasTime = false;
    private boolean hasIndex = false;

    private static final String KeyRAW = new String("_raw\0");
    private static final String KeyHOST = new String("MetaData:Host\0");
    private static final String KeySOURCETYPE = new String(
	    "MetaData:Sourcetype\0");
    private static final String KeySOURCE = new String("MetaData:Source\0");
    private static final String KeyTIME = new String("_time\0");
    private static final String KeyINDEX = new String("_MetaData:Index\0");

    private static final int BUFFER_SIZE = 56;
    private boolean debugging = false;
    private int channelID = -1;

    private static Logger logger = Logger.getLogger(EventParser.class);
    private EventThruput eventThruput = EventThruput.getInstance();

    public EventParser(InputStream sockIn, int id) {
	channelID = id;
	inStream = sockIn;
	reset();
    }

    public EventParser(InputStream sockIn, DataSink sender, int id) {
	channelID = id;
	emitter = sender;
	inStream = sockIn;
	reset();
    }

    public void debug() {
	// logger.setLevel(Level.DEBUG);
	logger.debug("set debug mode");
	debugging = true;
    }

    public void reset() {
	logger.debug("channel " + channelID + " Ready to receive new event");
	rawValue = null;
	rawLength = null;
	hostValue = null;
	sourceValue = null;
	sourceTypeValue = null;
	timeValue = null;

	hasRaw = false;
	hasHost = false;
	hasSource = false;
	hasSourceType = false;
	hasTime = false;
    }

    public boolean hasAllData() {
	if (!hasRaw)
	    return false;
	if (!hasHost)
	    return false;
	if (!hasSource)
	    return false;
	if (!hasSourceType)
	    return false;
	if (!hasIndex)
	    return false;

	return true;
    }

    public void outputEvent() {
	System.out.println("event_begin{");
	if (hasRaw) {
	    System.out.println(rawLength);
	    System.out.println(rawValue);
	} else {
	    System.out.println("10\n");
	}

	if (hasHost)
	    System.out.println(hostValue);
	else
	    System.out.println("host::");
	if (hasSourceType)
	    System.out.println(sourceTypeValue);
	else
	    System.out.println("sourcetype::");
	if (hasSource)
	    System.out.println(sourceValue);
	else
	    System.out.println("source::");
	if (hasTime)
	    System.out.println(timeValue);
	else
	    System.out.println("0");
	System.out.println("}event_end");

	if (debugging)
	    System.err.println("\n-----------------------------------------\n");
    }

    public void sendEvent() {
	try {

	    byte[] rawBuffer = new byte[0];
	    if (hasRaw) {
		rawBuffer = rawValue.getBytes("US-ASCII");
	    }

	    String host = "";
	    if (hasHost) {
		int idx = hostValue.indexOf("::");
		if (idx >= 0)
		    host = hostValue.substring(idx + 2);
	    }

	    String sourcetype = "";
	    if (hasSourceType) {
		int idx = sourceTypeValue.indexOf("::");
		if (idx >= 0)
		    sourcetype = sourceTypeValue.substring(idx + 2);
	    }

	    String source = "";
	    if (hasSource) {
		int idx = sourceValue.indexOf("::");
		if (idx >= 0)
		    source = sourceValue.substring(idx + 2);
	    }
	    source = source.replaceAll(":", "_");
	    source = source.replaceAll("\\s+", "_");
	    source = source.replaceAll("\\\\", "_");
	    source = source.replaceAll("/", "_");

	    long time = 0;
	    if (hasTime) {
		try {
		    time = Long.parseLong(timeValue);
		} catch (java.lang.NumberFormatException ex) {
		    ex.printStackTrace();
		}
	    }

	    logger.info("Parsed event: host = " + host + ", source = " + source
		    + ", sourcetype = " + sourcetype + ", time = " + time);
	    emitter.send(rawBuffer, sourcetype, source, host, time);

	} catch (Exception ex) {
	    logger.error("Failed parsing event: " + ex.toString()
		    + "\nStacktrace:\n" + ex.getStackTrace().toString());
	}

    }

    // receive event data
    public void readData() throws IOException {
	while (true) {
	    reset();
	    logger.info("channel " + channelID + " waiting for new event...");
	    int dataSize = getDataSize();
	    logger.debug("channel " + channelID + " data size = " + dataSize);
	    if (dataSize > 0)
		getData(dataSize);
	}
    }

    // print data collected from wire.
    public void displayData() {
	int count = 0;
	try {
	    while ((count = inStream.available()) > 0) {
		byte[] buffer = new byte[BUFFER_SIZE];

		if (count >= buffer.length) {
		    count = buffer.length;
		}

		count = inStream.read(buffer, 0, count);
		logger.trace("received " + count + " bytes");
		String msg = new String(buffer);
		System.err.println(msg);
	    }
	} catch (Exception ex) {
	    logger.error("Failed displaying event: " + ex.toString()
		    + "\nStacktrace:\n" + ex.getStackTrace().toString());
	}
    }

    public int getDataSize() throws IOException {
	if (debugging)
	    System.err.println("DEBUG: channel " + channelID
		    + " Receiving data size ...");

	int dataSize = -1;
	try {
	    int rawlen = 4;
	    byte[] raw_char = new byte[rawlen];
	    logger.trace("Initial byte array: " + Arrays.toString(raw_char));
	    int readLen = 0;
	    do {
		int numBytes = inStream.read(raw_char, readLen,
			(rawlen - readLen));
		if (numBytes < 0)
		    throw (new IOException("channel " + channelID
			    + " lost client connection"));

		readLen += numBytes;
		// String msg = Arrays.toString(raw_char); // new
		// String(raw_char);
		// System.err.println("recvd size msg of " + readLen +
		// " bytes so far: " + msg);
	    } while (readLen < rawlen);

	    ByteBuffer bb = ByteBuffer.wrap(raw_char);
	    IntBuffer ib = bb.asIntBuffer();
	    dataSize = ib.get(0);
	    logger.trace("recvd size = " + dataSize);
	} catch (IOException ex) {
	    logger.warn("channel " + channelID + " got IO exception");
	    throw ex;
	} catch (Exception ex) {
	    logger.error("Failed parsing data size: " + ex.toString()
		    + "\nStacktrace:\n" + ex.getStackTrace().toString());
	}

	return dataSize;
    }

    public void getData(int size) throws IOException {
	logger.debug("channel " + channelID + "rReceiving data of " + size
		+ " bytes ...");

	byte[] buf = new byte[size];
	int readLen = 0;

	try {
	    do {
		int numBytes = inStream.read(buf, readLen, (size - readLen));
		if (numBytes < 0)
		    throw (new IOException("Lost client connection"));

		readLen += numBytes;
		logger.trace("received data size so far = " + readLen);
	    } while (readLen < size);

	    logger.debug(new String(buf));
	} catch (IOException ex) {
	    logger.warn("channel " + channelID + " got IO exception");
	    throw ex;
	} catch (Exception ex) {
	    logger.error("Failed parsing data: " + ex.toString()
		    + "\nStacktrace:\n" + ex.getStackTrace().toString());
	}

	if (readLen < 4)
	    return;

	int count = parseCount(buf, 0);
	if (count > 0) {
	    int offset = 4;
	    processData(buf, offset, readLen - offset, count);
	}
    }

    /**
     * Convenient method for extracting all the stuff from serialized
     * CowPipelineData
     * 
     * @param buf
     *            - buf is serialized CowPipelineData
     */
    public void processS2SEvent(byte[] buf) {
	int count = parseCount(buf, 0);
	if (count > 0) {
	    int offset = 4;
	    processData(buf, offset, buf.length - offset, count);
	}
    }

    public void processData(byte[] buf, int offset, int size, int count) {
	int newOffset = offset;
	int newSize = size;
	boolean ignore = false;

	// Not entirely sure if _MetaData:Index is always present
	// loop till we find all keys or reach end
	for (int i = 0; (i < count) && !hasAllData(); ++i) {
	    ignore = false;
	    int len = parseField(buf, newOffset, newSize);
	    if (len < 0) {
		ignore = true;
		break;
	    }

	    newSize -= len;
	    newOffset += len;

	    if (newSize < 4)
		break;
	}

	if ((!ignore) && hasAllData()) {
	    if (emitter == null)
		outputEvent();
	    else
		sendEvent();

	    eventThruput.update(hostValue, sourceValue, sourceTypeValue,
		    indexValue, size);
	} else
	    logger.debug("Filtered out 1 event");
    }

    private int parseCount(byte[] buf, int offset) {
	byte[] countBuf = Arrays.copyOfRange(buf, offset, offset + 4);
	IntBuffer ib = ByteBuffer.wrap(countBuf).asIntBuffer();
	int count = ib.get(0);
	logger.trace("field count = " + count);
	return count;
    }

    // return the total length of the field.
    private int parseField(byte[] buf, int offset, int size) {
	// System.out.println("Parse field from " + offset + " of buffer size "
	// + size );

	String key = parseString(buf, offset, size);
	if (key == null)
	    return -1;

	// System.out.println("key = " + key);

	int len = (4 + key.length());

	String value = parseString(buf, offset + len, size - len);
	if (value == null)
	    return -1;

	// System.out.println("value = " + value);

	if (key.equals(KeyRAW)) {
	    if (value.charAt(value.length() - 1) == '\0') {
		rawValue = value.substring(0, value.length() - 1);
	    } else {
		rawValue = value;
	    }

	    if (rawValue.length() == 0)
		return -1; // drop 0 byte event.

	    rawLength = String.valueOf(rawValue.length());
	    // rawBuffer = Arrays.copyOfRange(buf, offset+len,
	    // rawValue.length());
	    hasRaw = true;
	} else if (key.equals(KeyHOST)) {
	    if (value.charAt(value.length() - 1) == '\0')
		hostValue = value.substring(0, value.length() - 1);
	    else
		hostValue = value;
	    hasHost = true;
	} else if (key.equals(KeySOURCETYPE)) {
	    if (value.charAt(value.length() - 1) == '\0')
		sourceTypeValue = value.substring(0, value.length() - 1);
	    else
		sourceTypeValue = value;

	    if (sourceTypeValue.equals("sourcetype::fwd-hb"))
		return -1; // drop hear-beat msg.

	    if (sourceTypeValue.equals("sourcetype::audittrail"))
		return -1; // drop audit msg.

	    hasSourceType = true;
	} else if (key.equals(KeySOURCE)) {
	    if (value.charAt(value.length() - 1) == '\0')
		sourceValue = value.substring(0, value.length() - 1);
	    else
		sourceValue = value;

	    if (sourceValue.indexOf("HadoopConnector") >= 0)
		return -1; // drop connector log messages.

	    if (sourceValue.equals("source::fwd-hb")
		    || sourceValue.equals("source::fwd"))
		return -1; // drop hear-beat msg.

	    if (sourceValue.equals("source::audittrail"))
		return -1; // drop audit msg.

	    hasSource = true;
	} else if (key.equals(KeyTIME)) {
	    if (value.charAt(value.length() - 1) == '\0')
		timeValue = value.substring(0, value.length() - 1);
	    else
		timeValue = value;
	    long unixtime = 0;
	    try {
		unixtime = Long.parseLong(timeValue);
		unixtime *= 1000L;
	    } catch (java.lang.NumberFormatException ex) {
		ex.printStackTrace();
	    }
	    timeValue = String.valueOf(unixtime);
	    hasTime = true;
	} else if (key.equals(KeyINDEX)) {
	    if (value.charAt(value.length() - 1) == '\0')
		indexValue = value.substring(0, value.length() - 1);
	    else
		indexValue = value;

	    if (indexValue.indexOf("hdfsconnector") >= 0)
		return -1; // drop connector log messages.

	    if (indexValue.equals("_internal") || indexValue.equals("_audit"))
		return -1; // drop internal msg.

	    hasIndex = true;
	}

	return (len + 4 + value.length());
    }

    private String parseString(byte[] buf, int offset, int size) {
	try {
	    int len = parseCount(buf, offset);
	    if (len <= 0)
		return null;
	    size -= 4;
	    if (size < len)
		return null;

	    String str = new String(buf, offset + 4, len, "UTF-8");
	    logger.trace("Parsed string = " + str);

	    return str;
	} catch (Exception ex) {
	    logger.error("Failed parsing data string: " + ex.toString()
		    + "\nStacktrace:\n" + ex.getStackTrace().toString());
	}

	return null;
    }

    // return bytes will always be 4 bytes long.
    public byte[] htonl(int x) {
	byte[] res = new byte[4];
	for (int i = 0; i < 4; i++) {
	    res[i] = (new Integer(x >>> 24)).byteValue();
	    x <<= 8;
	}
	return res;
    }

    // You can only ask for a byte array 4 bytes long to be converted. Rest
    // everything will be ignored.
    public int ntohl(byte[] x) {
	int res = 0;
	for (int i = 0; i < 4; i++) {
	    res <<= 8;
	    res |= (int) x[i];
	}
	return res;
    }

    /**
     * Convert the byte array to an int starting from the given offset.
     * 
     * @param b
     *            The byte array
     * @param offset
     *            The array offset
     * @return The integer
     * 
     *         ByteBuffer bb = ByteBuffer.wrap(new byte[] {0, 0, 0, 1, 0, 0, 0,
     *         4}); IntBuffer ib = bb.asIntBuffer(); int i0 = ib.get(0); int i1
     *         = ib.get(1);
     * 
     *         System.out.println(i0); System.out.println(i1);
     */
    public static int byteArrayToInt(byte[] b, int offset) {
	int value = 0;
	for (int i = 0; i < 4; i++) {
	    int shift = (4 - 1 - i) * 8;
	    value += (b[i + offset] & 0x000000FF) << shift;
	}
	return value;
    }
}