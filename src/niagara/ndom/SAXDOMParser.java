/**
 * $Id: SAXDOMParser.java,v 1.1 2002/03/26 22:07:41 vpapad Exp $
 *
 */

package niagara.ndom;

import org.w3c.dom.Document;

import java.io.IOException; 
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.SAXException;
import org.xml.sax.InputSource;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.SAXParseException;


import niagara.ndom.saxdom.*;

/**
 * <code>SAXDOMParser</code> constructs read-only SAXDOM documents
 * from a SAX source
 * @author <a href="mailto:vpapad@king.cse.ogi.edu">Vassilis Papadimos</a>
 */
public class SAXDOMParser implements niagara.ndom.DOMParser {

    private SAXParser parser;
    private SAXDOMHandler sh;

    public SAXDOMParser() {
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            parser = factory.newSAXParser();
            sh = new SAXDOMHandler();
        } 
        catch (FactoryConfigurationError e) {
            System.err.println("SAXDOMParser: Unable to get a factory.");
        } 
        catch (ParserConfigurationException e) {
            System.err.println("SAXDOMParser: Unable to configure parser.");
        }
        catch (SAXException e) {
            // parsing error
            System.err.println("SAXDOMParser: Got SAX Exception.");
        } 
    }    
    public void parse(InputSource is) throws SAXException, IOException {
        sh.reset();
        parser.parse(is, sh);
    }

    public Document getDocument() {
        return sh.getDocument();
    }

    public boolean hasErrors() {
        return sh.hasErrors();
    }
    
    public boolean hasWarnings() {
        return sh.hasWarnings();
    }

}

class SAXDOMHandler extends DefaultHandler {
    private DocumentImpl doc;
    private Page page;
    private int index;

    public SAXDOMHandler() {
        reset();
    }

    public Document getDocument() {
        return doc;
    }

    // Event handling
    
    public void startDocument() throws SAXException {
        if (page == null) {
            page = BufferManager.getFreePage();
            index = 0;
        }

        doc = new DocumentImpl(page, index);
        page.addEvent(doc, SAXEvent.START_DOCUMENT, null);
    }

    public void endDocument() throws SAXException {
        page.addEvent(doc, SAXEvent.END_DOCUMENT, null);
    }

    public void startElement(String namespaceURI, String localName, 
                             String qName, Attributes attrs) 
        throws SAXException {
        // XXX vpapad: not doing anything about namespaces yet
        page.addEvent(doc, SAXEvent.START_ELEMENT, localName);

        for (int i = 0; i < attrs.getLength(); i++) {
            page.addEvent(doc, SAXEvent.ATTR_NAME, attrs.getLocalName(i));
            page.addEvent(doc, SAXEvent.ATTR_VALUE, attrs.getValue(i));
        }
    }

    public void endElement(String namespaceURI, String localName, String qName)
        throws SAXException {
        page.addEvent(doc, SAXEvent.END_ELEMENT, null);
    }

    public void startPrefixMapping(String prefix, String uri) 
        throws SAXException {
    }

    public void endPrefixMapping(String prefix, String uri) 
        throws SAXException {
    }

    public void processingInstruction(String target, String data) 
        throws SAXException {
    }
    public void characters(char[] ch, int start,int length) 
        throws SAXException {
        // XXX vpapad: here we should probably normalize consecutive
        // TEXT elements into one
        page.addEvent(doc, SAXEvent.TEXT, new String(ch, start, length));
    }
    // Error handling
    private boolean hasErrors, hasWarnings;

    public boolean hasWarnings() {
        return hasWarnings;
    }

    public boolean hasErrors() {
        return hasErrors;
    }

    public void reset() {
        hasErrors = hasWarnings = false;
    }

    public void error(SAXParseException e) throws SAXException {
        hasErrors = true;
    }
    public void fatalError(SAXParseException e) throws SAXException {
        hasErrors = true;
    }

    public void warning(SAXParseException e) throws SAXException {
        hasWarnings = true;
    }
}
