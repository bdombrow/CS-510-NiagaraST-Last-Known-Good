/**
 * $Id: SAXDOMParser.java,v 1.4 2002/03/27 23:36:59 vpapad Exp $
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

    public boolean supportsStreaming() {
	return false;
    }
}

class SAXDOMHandler extends DefaultHandler {
    private DocumentImpl doc;
    private Page page;
    private int offset;

    private StringBuffer sb;

    public SAXDOMHandler() {
        reset();
    }

    public Document getDocument() {
        return doc;
    }

    // Event handling
    
    public void startDocument() throws SAXException {
        if (page == null || page.isFull()) {
            page = BufferManager.getFreePage();
        }
        
        offset = page.getCurrentOffset();

        doc = new DocumentImpl(page, offset);
    }

    public void endDocument() throws SAXException {
        handleText();
        page.addEvent(doc, SAXEvent.END_DOCUMENT, null);
    }

    public void startElement(String namespaceURI, String localName, 
                             String qName, Attributes attrs) 
        throws SAXException {
        handleText();
        // XXX vpapad: not doing anything about namespaces yet
        page.addEvent(doc, SAXEvent.START_ELEMENT, qName);

        for (int i = 0; i < attrs.getLength(); i++) {
            page.addEvent(doc, SAXEvent.ATTR_NAME, attrs.getLocalName(i));
            page.addEvent(doc, SAXEvent.ATTR_VALUE, attrs.getValue(i));
        }
    }

    public void endElement(String namespaceURI, String localName, String qName)
        throws SAXException {
        handleText();
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
        sb.append(ch, start, length);
    }
    
    public void handleText() {
        page.addEvent(doc, SAXEvent.TEXT, sb.toString());
        sb.setLength(0);
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
        doc = null;
        hasErrors = hasWarnings = false;
        sb = new StringBuffer();
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
