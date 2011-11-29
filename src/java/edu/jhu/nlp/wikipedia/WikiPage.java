package edu.jhu.nlp.wikipedia;

import java.util.Vector;

import edu.jhu.nlp.language.Language;

/**
 * Data structures for a wikipedia page.
 * 
 * @author Delip Rao
 * 
 */
public class WikiPage {

    private String title = null;
    private WikiTextParser wikiTextParser = null;
    private String id = null;
    private String author = null;
    private String modifiedtime = null;

    /**
     * Set the page title. This is not intended for direct use.
     * 
     * @param title
     */
    public void setTitle(String title) {
	this.title = title;
    }

    /**
     * Set the wiki text associated with this page. This setter also introduces
     * side effects. This is not intended for direct use.
     * 
     * @param wtext
     *            wiki-formatted text
     */
    public void setWikiText(String wtext) {
	wikiTextParser = new WikiTextParser(wtext);
    }

    /**
     * 
     * @return a string containing the page title.
     */
    public String getTitle() {
	return title;
    }

    /**
     * 
     * @param languageCode
     * @return a string containing the title translated in the given
     *         languageCode.
     * @see Language
     */
    public String getTranslatedTitle(String languageCode) {
	return wikiTextParser.getTranslatedTitle(languageCode);
    }

    /**
     * 
     * @return true if this a disambiguation page.
     */
    public boolean isDisambiguationPage() {
	if (title.contains("(disambiguation)")
		|| wikiTextParser.isDisambiguationPage())
	    return true;
	else
	    return false;
    }

    /**
     * 
     * @return true for "special pages" -- like Category:, Wikipedia:, etc
     */
    public boolean isSpecialPage() {
	return title.contains(":");
    }

    /**
     * Use this method to get the wiki text associated with this page. Useful
     * for custom processing the wiki text.
     * 
     * @return a string containing the wiki text.
     */
    public String getWikiText() {
	return wikiTextParser.getText();
    }

    /**
     * 
     * @return true if this is a redirection page
     */
    public boolean isRedirect() {
	return wikiTextParser.isRedirect();
    }

    /**
     * 
     * @return true if this is a stub page
     */
    public boolean isStub() {
	return wikiTextParser.isStub();
    }

    /**
     * 
     * @return the title of the page being redirected to.
     */
    public String getRedirectPage() {
	return wikiTextParser.getRedirectText();
    }

    /**
     * 
     * @return plain text stripped of all wiki formatting.
     */
    public String getText() {
	return wikiTextParser.getPlainText();
    }

    /**
     * 
     * @return a list of categories the page belongs to, null if this a
     *         redirection/disambiguation page
     */
    public Vector<String> getCategories() {
	return wikiTextParser.getCategories();
    }

    /**
     * 
     * @return a list of links contained in the page
     */
    public Vector<String> getLinks() {
	return wikiTextParser.getLinks();
    }

    public void setID(String id) {
	this.id = id;
    }

    public InfoBox getInfoBox() {
	return wikiTextParser.getInfoBox();
    }

    public String getID() {
	return id;
    }

    public String getAuthor() {
	return author;
    }

    public void setAuthor(String author) {
	this.author = author;
    }

    public String getModifiedTime() {
	return this.modifiedtime;
    }

    public void setModifiedTime(String mtime) {
	this.modifiedtime = mtime;
    }
}
