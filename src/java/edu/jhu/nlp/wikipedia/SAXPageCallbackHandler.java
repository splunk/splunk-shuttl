package edu.jhu.nlp.wikipedia;

import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

/**
 * 
 * A Wrapper class for the PageCallbackHandler
 * 
 * @author Jason Smith
 *
 */
public class SAXPageCallbackHandler extends DefaultHandler {

	private PageCallbackHandler pageHandler;
	private WikiPage currentPage;
	private String currentTag;
	
	private StringBuilder currentWikitext;
	private StringBuilder currentTitle;
	private StringBuilder currentID;
	private StringBuilder currentAuthor;
	private StringBuilder currentModifiedTime;
	
	public SAXPageCallbackHandler(PageCallbackHandler ph){
		pageHandler = ph;
	}
	
	public void startElement(String uri, String name, String qName, Attributes attr){
		currentTag = qName;
		if (qName.equals("page")){
			currentPage = new WikiPage();
			currentWikitext = new StringBuilder("");
			currentTitle = new StringBuilder("");
			currentID = new StringBuilder("");
			currentAuthor = new StringBuilder("");
			currentModifiedTime = new StringBuilder("");
		}
	}
	
	public void endElement(String uri, String name, String qName){
		if (qName.equals("page")){
			currentPage.setTitle(currentTitle.toString());
			currentPage.setID(currentID.toString());
			currentPage.setWikiText(currentWikitext.toString());
			currentPage.setAuthor(currentAuthor.toString());
			currentPage.setModifiedTime(currentModifiedTime.toString());
			pageHandler.process(currentPage);
		}
		if (qName.equals("mediawiki"))
		{
			// TODO hasMoreElements() should now return false
		}
	}
	
	public void characters(char ch[], int start, int length){
		if (currentTag.equals("title")){
			currentTitle = currentTitle.append(ch, start, length);
		}
		// TODO: To avoid looking at the revision ID, only the first ID is taken.
		// I'm not sure how big the block size is in each call to characters(),
		// so this may be unsafe.
		else if ((currentTag.equals("id")) && (currentID.length() == 0)){
			currentID = new StringBuilder();
			currentID.append(ch, start, length);
		}
		else if (currentTag.equals("text")){
			currentWikitext = currentWikitext.append(ch, start, length);
		}
		else if (currentTag.equals("username")) {
			currentAuthor = currentAuthor.append(ch, start, length);
		}
		else if (currentTag.equals("timestamp")) {
			currentModifiedTime = currentModifiedTime.append(ch, start, length);
		}
	}
}
