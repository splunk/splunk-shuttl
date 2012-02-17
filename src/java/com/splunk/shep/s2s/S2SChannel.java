package com.splunk.shep.s2s;

import org.apache.log4j.Logger;

public class S2SChannel {
    private S2SStateMachine stateMachine;
    private Logger logger = Logger.getLogger(getClass());

    public S2SChannel(S2SProtocolHandler handler) {
	stateMachine = new S2SStateMachine(handler);
    }

    public void dataAvailable(byte[] buf, int offset, int len) throws Exception {
	try {
	    stateMachine.consume(buf, offset, len);
	} catch (Exception ex) {
	    logger.error(ex);
	    throw new Exception(ex.getMessage());
	}
    }
}
