// S2SStateMachine.java
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

package com.splunk.shep.s2s;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

public class S2SStateMachine {
    private State currentState;

    public S2SStateMachine(S2SProtocolHandler callback) {
	initStateMachine(callback);
    }

    private void initStateMachine(S2SProtocolHandler callback) {
	State sigState = new SignatureReadingState();
	LengthReadingState lenReadingState = new LengthReadingState();
	RawDataReadingState rawDataReadingState = new RawDataReadingState(
		callback);
	lenReadingState.setRawDataReadingState(rawDataReadingState);

	sigState.setNextState(lenReadingState);
	lenReadingState.setNextState(rawDataReadingState);
	rawDataReadingState.setNextState(lenReadingState);

	this.currentState = sigState;
    }

    public void consume(byte[] buf, int startIndex, int bufLen)
	    throws InvalidSignatureException, InvalidS2SDataException,
	    Exception {
	StateContext ctxt = new StateContext(buf, startIndex, bufLen);
	while (!currentState.consume(ctxt)) {
	    // fully consumed
	    currentState = currentState.getNextState();
	}
    }
}

class StateContext {
    // public int consumed = 0;
    public byte[] buf;
    public int startIndex;
    public int bufLen;

    public StateContext(byte[] buf, int startIndex, int bufLen) {
	this.buf = buf;
	this.startIndex = startIndex;
	this.bufLen = bufLen;
    }
}

interface State {
    public String getName();

    public boolean consume(StateContext ctxt) throws InvalidSignatureException,
	    InvalidS2SDataException, Exception;

    public State getNextState();

    public void setNextState(State state);
}

abstract class AbstractState implements State {
    private String name;
    protected int sizeExpectedData = 0;
    protected int sizeDataReceived = 0;
    protected State nextState;

    public AbstractState(String name) {
	this.name = name;
    }

    public String getName() {
	return name;
    }

    public boolean doConsume(StateContext ctxt, byte[] dest) {
	int need = sizeExpectedData - sizeDataReceived;
	int available = ctxt.bufLen - ctxt.startIndex;
	if (need <= available) {
	    System.arraycopy(ctxt.buf, ctxt.startIndex, dest, sizeDataReceived,
		    need);
	    ctxt.startIndex += need;
	    sizeDataReceived += need;
	    return false;
	} else {
	    System.arraycopy(ctxt.buf, ctxt.startIndex, dest, sizeDataReceived,
		    available);
	    ctxt.startIndex += available;
	    sizeDataReceived += available;
	    return true;
	}
    }

    public State getNextState() {
	return nextState;
    }

    public void setNextState(State state) {
	this.nextState = state;
    }
}

class SignatureReadingState extends AbstractState {
    private static final String TCPSIGNATURE_COOKED_SIG = new String(
	    "--splunk-cooked-mode--");
    private static final String TCPSIGNATURE_COOKED_SIG_V2 = new String(
	    "--splunk-cooked-mode-v2--");
    private static final String TCPSIGNATURE_COOKED_SIG_V3 = new String(
	    "--splunk-cooked-mode-v3--");
    private byte[] sig = new byte[400];
    Logger logger = Logger.getLogger(getClass());

    public SignatureReadingState() {
	super("SignatureReadingState");
	sizeExpectedData = 400;
    }

    @Override
    public boolean consume(StateContext ctxt) throws InvalidSignatureException {
	boolean rv = doConsume(ctxt, sig);
	if (!rv) {
	    verifySignature(sig);
	}
	return rv;
    }

    private void verifySignature(byte[] signature)
	    throws InvalidSignatureException {
	String sig = new String(signature, 0, 128);
	logger.debug("Verifying signature : " + sig);
	if (sig.startsWith(TCPSIGNATURE_COOKED_SIG_V3)) {
	    throw new InvalidSignatureException(
		    "V3 protocol not supported at this time.");
	} else if (sig.startsWith(TCPSIGNATURE_COOKED_SIG_V2)) {
	    // fine
	    logger.debug("Sinature : " + sig + " received");
	} else if (sig.startsWith(TCPSIGNATURE_COOKED_SIG)) {
	    // fine
	    logger.debug("Sinature : " + sig + " received");
	} else {
	    throw new InvalidSignatureException("Unsupported signature " + sig
		    + " received");
	}
    }
}

class LengthReadingState extends AbstractState {
    private byte[] len = new byte[4];
    private int size;
    private RawDataReadingState rawDataReadingState;

    public LengthReadingState() {
	super("LengthReadingState");
	sizeExpectedData = 4;
    }

    public void setRawDataReadingState(RawDataReadingState rawDataReadingState) {
	this.rawDataReadingState = rawDataReadingState;
    }

    public void setSizeNeeded(int size) {
	sizeExpectedData = size;
    }

    @Override
    public boolean consume(StateContext ctxt) throws InvalidS2SDataException {
	boolean rv = doConsume(ctxt, len);
	if (!rv) {
	    ByteBuffer byteBuf = ByteBuffer.wrap(len);
	    size = byteBuf.getInt();
	    if (size > 64000000) {
		throw new InvalidS2SDataException("Bad data received");
	    }
	    if (size == 0) {
		throw new InvalidS2SDataException(
			"Could not parse size of data properly");
	    }

	    rawDataReadingState.setSizeNeeded(size);
	    sizeDataReceived = 0;
	}
	return rv;
    }

}

class RawDataReadingState extends AbstractState {
    private byte[] raw;
    private S2SProtocolHandler callback;

    public RawDataReadingState(S2SProtocolHandler callback) {
	super("RawDataReadingState");
	this.callback = callback;
    }

    public void setSizeNeeded(int size) {
	sizeExpectedData = size;
	raw = new byte[size];
    }

    @Override
    public boolean consume(StateContext ctxt) throws Exception {
	boolean rv = doConsume(ctxt, raw);
	if (!rv) {
	    callback.s2sDataAvailable(raw);
	    sizeDataReceived = 0;
	    sizeExpectedData = 0;
	}
	return rv;
    }

}