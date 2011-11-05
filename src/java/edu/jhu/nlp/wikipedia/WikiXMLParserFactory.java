package edu.jhu.nlp.wikipedia;

/**
 * 
 * @author Delip Rao
 *
 */
public class WikiXMLParserFactory {
	
	public static WikiXMLParser getDOMParser(String fileName) {
		return new WikiXMLDOMParser(fileName);
	}
	
	public static WikiXMLParser getSAXParser(String fileName) {
		return new WikiXMLSAXParser(fileName);
	}
	
}
