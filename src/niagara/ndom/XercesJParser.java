/**
 * $Id: XercesJParser.java,v 1.1 2001/07/17 03:36:37 vpapad Exp $
 *
 */

package niagara.ndom;

import org.w3c.dom.Document;
import java.io.IOException;

import org.xml.sax.*;
import org.xml.sax.helpers.*;

import org.apache.xerces.parsers.*;

/**
 * <code>XercesJParser</code> is a wrapper for org.apache.xerces.parsers.DOMParser;
 *
 * @author <a href="mailto:vpapad@king.cse.ogi.edu">Vassilis Papadimos</a>
 */
public class XercesJParser implements niagara.ndom.DOMParser {

    private org.apache.xerces.parsers.DOMParser parser;
    private SimpleHandler sh;

    public XercesJParser() {
        parser = new org.apache.xerces.parsers.DOMParser();
        sh = new SimpleHandler();
    }

    public void parse(InputSource is) throws SAXException, IOException {
        sh.reset();
        parser.setErrorHandler(sh);
        parser.parse(is);
    }

    public Document getDocument() {
        return parser.getDocument();
    }

    public boolean hasErrors() {
        return sh.hasErrors();
    }
    
    public boolean hasWarnings() {
        return sh.hasWarnings();
    }

}

class SimpleHandler extends DefaultHandler {
    private boolean hasErrors, hasWarnings;

    public SimpleHandler() {
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