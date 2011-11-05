package edu.jhu.nlp.wikipedia;

import java.io.InputStream;

import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 * 
 * A SAX Parser for Wikipedia XML dumps.  
 * 
 * @author Jason Smith
 *
 */
public class WikiXMLSAXParser extends WikiXMLParser {

	private XMLReader xmlReader;
	private PageCallbackHandler pageHandler = null;

	public WikiXMLSAXParser(InputStream is){
		super(is);
		this.initReaderHandler();		
	}
	
	public WikiXMLSAXParser(String fileName){
		super(fileName);
		this.initReaderHandler();				
	}

	private void initReaderHandler(){
		try {
			xmlReader = XMLReaderFactory.createXMLReader();
			pageHandler = new IteratorHandler(this);
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Set a callback handler. The callback is executed every time a
	 * page instance is detected in the stream. Custom handlers are
	 * implementations of {@link PageCallbackHandler}
	 * @param handler
	 * @throws Exception
	 */
	public void setPageCallback(PageCallbackHandler handler) throws Exception {
		pageHandler = handler;
	}

	/**
	 * The main parse method.
	 * @throws Exception
	 */
	public void parse()  throws Exception  {
		xmlReader.setContentHandler(new SAXPageCallbackHandler(pageHandler));
		xmlReader.parse(getInputSource());
	}

	/**
	 * This parser is event driven, so it
	 * can't provide a page iterator.
	 */
	@Override
	public WikiPageIterator getIterator() throws Exception {
		if(!(pageHandler instanceof IteratorHandler)) {
			throw new Exception("Custom page callback found. Will not iterate.");
		}
		throw new UnsupportedOperationException();
	}
	
	/**
	 * A convenience method for the Wikipedia SAX interface
	 * @param dumpFile - path to the Wikipedia dump
	 * @param handler - callback handler used for parsing
	 * @throws Exception
	 */
	public static void parseWikipediaDump(String dumpFile, 
	    PageCallbackHandler handler) throws Exception {
	  WikiXMLParser wxsp = WikiXMLParserFactory.getSAXParser(dumpFile);
	  wxsp.setPageCallback(handler);
	  wxsp.parse();
	}
	
}
