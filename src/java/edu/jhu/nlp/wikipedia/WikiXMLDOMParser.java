package edu.jhu.nlp.wikipedia;

import java.io.InputStream;
import java.util.Vector;

import org.apache.xerces.parsers.DOMParser;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * A memory efficient parser for easy access to Wikipedia XML dumps in native and compressed XML formats.<br>
 * 
 * Typical pattern of use:<p>
 * <code>
 * WikiXMLDOMParser wxp = new WikiXMLDOMParser("enwiki-latest-pages-articles.xml");<br>
 * wxp.setPageCallback(...);<br>
 * wxp.parse();<br>
 * </code><p>
 * or<p>
 * <code>
 * WikiXMLDOMParser wxp = new WikiXMLDOMParser("enwiki-latest-pages-articles.xml");<br>
 * wxp.parse();<br>
 * WikiPageIterator it = wxp.getIterator();<br>
 * ...
 * </code>
 * @author Delip Rao
 *
 */
public class WikiXMLDOMParser extends WikiXMLParser {
	
	private DOMParser domParser = new DOMParser();
	private static String FEATURE_URI = 
		"http://apache.org/xml/features/dom/defer-node-expansion";
	private Vector<WikiPage> pageList = null;
	private PageCallbackHandler pageHandler = null;
		
	public WikiXMLDOMParser(String fileName){
		super(fileName);
	}
	
	public WikiXMLDOMParser(InputStream is){
		super(is);
	}
	
	/**
	 * Set a callback handler. The callback is executed every time a
	 * page instance is detected in the stream. Custom handlers are
	 * implementations of {@link PageCallbackHandler}
	 * @param handler
	 * @throws Exception
	 */
	public void setPageCallback(PageCallbackHandler handler) throws Exception {
		if(pageList != null) throw new Exception("Set the callback before calling parse()");
		pageHandler = handler;
	}
	
	/**
	 * 
	 * @return an iterator to the list of pages
	 * @throws Exception
	 */
	public WikiPageIterator getIterator() throws Exception {
		if(pageHandler != null) throw new Exception("page callback found. Cannot iterate.");
		return new WikiPageIterator(pageList);
	}
	
	/**
	 * The main parse method.
	 * @throws Exception
	 */
	public void parse()  throws Exception  {
		
		if(pageHandler == null)
			pageList = new Vector<WikiPage>();
		
		domParser.setFeature(FEATURE_URI, true);

		domParser.parse(this.getInputSource());
		
		Document doc = domParser.getDocument();
		NodeList pages = doc.getElementsByTagName("page");
		
		for(int i = 0; i < pages.getLength(); i++) {
			WikiPage wpage = new WikiPage();
			Node pageNode = pages.item(i);
			NodeList childNodes = pageNode.getChildNodes();
			for(int j = 0; j < childNodes.getLength(); j++) {
				Node child = childNodes.item(j);
				if(child.getNodeName().equals("title"))
					wpage.setTitle(child.getFirstChild().getNodeValue());
				else if(child.getNodeName().equals("id"))
					wpage.setID(child.getFirstChild().getNodeValue());
				else if(child.getNodeName().equals("revision")) {
					NodeList revchilds = child.getChildNodes();
					for(int k = 0; k < revchilds.getLength(); k++) {
						Node rchild = revchilds.item(k); 
						if(rchild.getNodeName().equals("text")) 
							wpage.setWikiText(rchild.getFirstChild().getNodeValue());
					}
				}
			}
			
			if(pageHandler != null) {
				pageHandler.process(wpage);
			} else pageList.add(wpage);
		}	
	}
}
