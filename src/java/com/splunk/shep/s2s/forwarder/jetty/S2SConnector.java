package com.splunk.shep.s2s.forwarder.jetty;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.eclipse.jetty.http.HttpSchemes;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.bio.SocketConnector;

public class S2SConnector extends SocketConnector {
    private static final String IGNORED = "ignored";
    private Logger logger = Logger.getLogger(getClass());
    private String sinkclassname;
    private String name;

    static String __secretWord = null;
    static boolean __allowShutdown = false;
    static int MAX_PACKET_SIZE = 40960;

    public S2SConnector() {
	logger.info("S2SConnector");
	super.setRequestHeaderSize(MAX_PACKET_SIZE);
	super.setResponseHeaderSize(MAX_PACKET_SIZE);
	super.setRequestBufferSize(MAX_PACKET_SIZE);
	super.setResponseBufferSize(MAX_PACKET_SIZE);
	// IN AJP protocol the socket stay open, so
	// by default the time out is set to 0 seconds
	super.setMaxIdleTime(0);
    }

    public String getName() {
	return name;
    }

    public void setName(String name) {
	this.name = name;
    }

    public void setDataSink(String classname) {
	this.sinkclassname = classname;
	logger.info("sink : " + classname);
    }

    public String getDataSink() {
	return this.sinkclassname;
    }

    @Override
    protected void doStart() throws Exception {
	super.doStart();
	logger.info("S2S is not a secure protocol. Please protect port :"
		+ Integer.toString(getLocalPort()));
    }

    /* ------------------------------------------------------------ */
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.eclipse.jetty.server.bio.SocketConnector#customize(org.eclipse.io
     * .EndPoint, org.eclipse.jetty.server.Request)
     */
    @Override
    public void customize(EndPoint endpoint, Request request)
	    throws IOException {
	super.customize(endpoint, request);
	if (request.isSecure())
	    request.setScheme(HttpSchemes.HTTPS);
    }

    /* ------------------------------------------------------------ */
    @Override
    protected Connection newConnection(EndPoint endpoint) {
	return new S2SConnection(this, endpoint, getServer());
    }

    /* ------------------------------------------------------------ */
    // Secured on a packet by packet bases not by connection
    @Override
    public boolean isConfidential(Request request) {
	return false;
    }

    /* ------------------------------------------------------------ */
    // Secured on a packet by packet bases not by connection
    @Override
    public boolean isIntegral(Request request) {
	return false;
    }

    /* ------------------------------------------------------------ */
    @Deprecated
    public void setHeaderBufferSize(int headerBufferSize) {
	logger.trace(IGNORED);
    }

    /* ------------------------------------------------------------ */
    @Override
    public void setRequestBufferSize(int requestBufferSize) {
	logger.trace(IGNORED);
    }

    /* ------------------------------------------------------------ */
    @Override
    public void setResponseBufferSize(int responseBufferSize) {
	logger.trace(IGNORED);
    }

    /* ------------------------------------------------------------ */
    public void setAllowShutdown(boolean allowShutdown) {
	logger.warn("S2S: Shutdown Request is: " + allowShutdown);
	__allowShutdown = allowShutdown;
    }

    /* ------------------------------------------------------------ */
    public void setSecretWord(String secretWord) {
	logger.warn("S2S: Shutdown Request secret word is : " + secretWord);
	__secretWord = secretWord;
    }
}
