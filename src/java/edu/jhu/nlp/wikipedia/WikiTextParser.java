package edu.jhu.nlp.wikipedia;

import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * For internal use only -- Used by the {@link WikiPage} class.
 * Can also be used as a stand alone class to parse wiki formatted text.
 * @author Delip Rao
 *
 */
public class WikiTextParser {
	
	private String wikiText = null;
	private Vector<String> pageCats = null;
	private Vector<String> pageLinks = null;
	private boolean redirect = false;
	private String redirectString = null;
	private static Pattern redirectPattern = 
	  Pattern.compile("#REDIRECT\\s+\\[\\[(.*?)\\]\\]");
	private boolean stub = false;
	private boolean disambiguation = false;
	private static Pattern stubPattern = Pattern.compile("\\-stub\\}\\}");
	private static Pattern disambCatPattern = Pattern.compile("\\{\\{disambig\\}\\}");
  private InfoBox infoBox = null;
	
	public WikiTextParser(String wtext) {
		wikiText = wtext;		
		Matcher matcher = redirectPattern.matcher(wikiText);
		if(matcher.find()) {
			redirect = true;
			if(matcher.groupCount() == 1)
				redirectString = matcher.group(1); 
		}
		matcher = stubPattern.matcher(wikiText);
		stub = matcher.find();
		matcher = disambCatPattern.matcher(wikiText);
		disambiguation = matcher.find();
	}
	
	public boolean isRedirect() {
		return redirect;
	}
	
	public boolean isStub() {
	  return stub;
	}
	
	public String getRedirectText() {
		return redirectString;
	}

	public String getText() {
		return wikiText;
	}

	public Vector<String> getCategories() {
		if(pageCats == null) parseCategories();
		return pageCats;
	}

	public Vector<String> getLinks() {
		if(pageLinks == null) parseLinks();
		return pageLinks;
	}

	private void parseCategories() {
		pageCats = new Vector<String>();
		Pattern catPattern = Pattern.compile("\\[\\[Category:(.*?)\\]\\]", Pattern.MULTILINE);
		Matcher matcher = catPattern.matcher(wikiText);
		while(matcher.find()) {
			String [] temp = matcher.group(1).split("\\|");
			pageCats.add(temp[0]);
		}
	}
	
	private void parseLinks() {
		pageLinks = new Vector<String>();  
		
		Pattern catPattern = Pattern.compile("\\[\\[(.*?)\\]\\]", Pattern.MULTILINE);
		Matcher matcher = catPattern.matcher(wikiText);
		while(matcher.find()) {
			String [] temp = matcher.group(1).split("\\|");
			if(temp == null || temp.length == 0) continue;
			String link = temp[0];
			if(link.contains(":") == false) {
				pageLinks.add(link);
			}
		}
	}

	public String getPlainText() {
		String text = wikiText.replaceAll("&gt;", ">");
    text = text.replaceAll("&lt;", "<");
    text = text.replaceAll("<ref>.*?</ref>", " ");
		text = text.replaceAll("</?.*?>", " ");
		text = text.replaceAll("\\{\\{.*?\\}\\}", " ");
		text = text.replaceAll("\\[\\[.*?:.*?\\]\\]", " ");
		text = text.replaceAll("\\[\\[(.*?)\\]\\]", "$1");
		text = text.replaceAll("\\s(.*?)\\|(\\w+\\s)", " $2");
		text = text.replaceAll("\\[.*?\\]", " ");
		text = text.replaceAll("\\'+", "");
		return text;
	}

  public InfoBox getInfoBox() {
    //parseInfoBox is expensive. Doing it only once like other parse* methods
    if(infoBox == null)
      infoBox = parseInfoBox();
    return infoBox;
  }

  private InfoBox parseInfoBox() {
    String INFOBOX_CONST_STR = "{{Infobox";
    int startPos = wikiText.indexOf(INFOBOX_CONST_STR);
    if(startPos < 0) return null;
    int bracketCount = 2;
    int endPos = startPos + INFOBOX_CONST_STR.length();
    for(; endPos < wikiText.length(); endPos++) {
      switch(wikiText.charAt(endPos)) {
        case '}':
          bracketCount--;
          break;
        case '{':
          bracketCount++;
          break;
        default:
      }
      if(bracketCount == 0) break;
    }
    if(endPos+1 >= wikiText.length()) return null;
    // This happens due to malformed Infoboxes in wiki text. See Issue #10
    // Giving up parsing is the easier thing to do.
    String infoBoxText = wikiText.substring(startPos, endPos+1);
    infoBoxText = stripCite(infoBoxText); // strip clumsy {{cite}} tags
    // strip any html formatting
    infoBoxText = infoBoxText.replaceAll("&gt;", ">");
    infoBoxText = infoBoxText.replaceAll("&lt;", "<");
    infoBoxText = infoBoxText.replaceAll("<ref.*?>.*?</ref>", " ");
		infoBoxText = infoBoxText.replaceAll("</?.*?>", " ");
    return new InfoBox(infoBoxText);
  }

  private String stripCite(String text) {
    String CITE_CONST_STR = "{{cite";
    int startPos = text.indexOf(CITE_CONST_STR);
    if(startPos < 0) return text;
    int bracketCount = 2;
    int endPos = startPos + CITE_CONST_STR.length();
    for(; endPos < text.length(); endPos++) {
      switch(text.charAt(endPos)) {
        case '}':
          bracketCount--;
          break;
        case '{':
          bracketCount++;
          break;
        default:
      }
      if(bracketCount == 0) break;
    }
    text = text.substring(0, startPos-1) + text.substring(endPos);
    return stripCite(text);   
  }

  public boolean isDisambiguationPage() {
    return disambiguation;
  }

  public String getTranslatedTitle(String languageCode) {
    Pattern pattern = Pattern.compile("^\\[\\[" + languageCode + ":(.*?)\\]\\]$", Pattern.MULTILINE);
    Matcher matcher = pattern.matcher(wikiText);
    if(matcher.find()) {
      return matcher.group(1);
    }
    return null;
  }

}
