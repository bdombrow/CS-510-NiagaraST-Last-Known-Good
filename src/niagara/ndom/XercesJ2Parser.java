/**
 * $Id: XercesJ2Parser.java,v 1.2 2002/10/31 04:29:27 vpapad Exp $
 *
 */

package niagara.ndom;

import org.w3c.dom.Document;
import java.io.IOException;

import org.xml.sax.*;
import org.xml.sax.helpers.*;

import javax.xml.parsers.*;

/**
 * <code>XercesJParser</code> is a wrapper for org.apache.xerces.parsers.DOMParser;
 */
public class XercesJ2Parser implements niagara.ndom.DOMParser {

    private DocumentBuilder parser;
    private SimpleHandler2 sh;
    private Document d;

    public XercesJ2Parser(DocumentBuilderFactory dbf) {

	try {
	    parser = dbf.newDocumentBuilder();
	    sh = new SimpleHandler2();
	} catch (javax.xml.parsers.ParserConfigurationException pce) {
	    System.err.println("Unable to create XercesJ2Parser " + 
			       pce.getMessage());
	}
    }

    public void parse(org.xml.sax.InputSource is) 
	throws SAXException, IOException {
        sh.reset();
        parser.setErrorHandler(sh);
        d = parser.parse(is);
    }

    public Document getDocument() {
        return d;
    }

    public boolean hasErrors() {
        return sh.hasErrors();
    }
    
    public boolean hasWarnings() {
        return sh.hasWarnings();
    }

    public boolean supportsStreaming() {
	return true;
    }

}

class SimpleHandler2 extends DefaultHandler {
    private boolean hasErrors, hasWarnings;

    public SimpleHandler2() {
        reset();
    }

    public boolean hasWarnings() {
        return hasWarnings;
    }

    public boolean hasErrors() {
        return hasErrors;
    }

    public void reset() {
        hasErrors = hasWarnings = false;
    }

    public void error(SAXParseException e) {
        hasErrors = true;
    }

    public void warning(SAXParseException e) {
        hasWarnings = true;
    }
}
