/**
 * $Id: SAXDOMParser.java,v 1.5 2002/03/31 15:56:38 tufte Exp $
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
import niagara.utils.*;
import niagara.connection_server.NiagraServer; // for STREAM variable

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

    // for streaming, a place to put top-level elements
    public void setOutputStream(SourceStream outputStream) {
	sh.setOutputStream(outputStream);
    }
}

class SAXDOMHandler extends DefaultHandler {
    private DocumentImpl doc;
    private Page page;

    private StringBuffer sb;

    // for streaming support - KT
    private int level;
    private SourceStream outputStream;
    private int levelOneStartIdx;

    public SAXDOMHandler() {
        reset();
    }

    public Document getDocument() {
        return doc;
    }

    // Event handling
    
    public void startDocument() throws SAXException {
	level = 0;
        if (page == null || page.isFull()) {
            page = BufferManager.getFreePage();
        }
        doc = new DocumentImpl(page, page.getCurrentOffset());
    }

    public void endDocument() throws SAXException {
        handleText();
        page = page.addEvent(doc, SAXEvent.END_DOCUMENT, null);
    }

    public void startElement(String namespaceURI, String localName, 
                             String qName, Attributes attrs) 
        throws SAXException {

        handleText();
        // XXX vpapad: not doing anything about namespaces yet

	page = page.addEvent(doc, SAXEvent.START_ELEMENT, qName);
	if(NiagraServer.STREAM) {
	    if(level == 1) {
		levelOneStartIdx = BufferManager.getIndex(page,
							  page.getCurrentOffset()-1);
	    }
	    level++;
	}

        for (int i = 0; i < attrs.getLength(); i++) {
	    page = page.addEvent(doc, SAXEvent.ATTR_NAME, 
				 attrs.getLocalName(i));
            page = page.addEvent(doc, SAXEvent.ATTR_VALUE, attrs.getValue(i));
        }
    }

    public void endElement(String namespaceURI, String localName, String qName)
        throws SAXException {
	level--;
        handleText();
        page = page.addEvent(doc, SAXEvent.END_ELEMENT, null);

	if(NiagraServer.STREAM) {
	    if(level == 1 && outputStream !=  null) {
		// for now throw fatal errors on these exceptions, if
		// they happen, I'll have to figure out what the right
		// thing is to do - KT
		// I don't want to do a lot of work until I know this
		// actually works
		try {
		    outputStream.put(new ElementImpl(doc, levelOneStartIdx));
		} catch (java.lang.InterruptedException ie) {
		    throw new PEException("KT - InterruptedException in SAXDOMParser");
		} catch (niagara.utils.NullElementException ne) {
		    throw new PEException("KT - NullElementException in SAXDOMParser");
		} catch (niagara.utils.StreamPreviouslyClosedException spce) {
		    throw new PEException("KT - StreamPreviouslyClosedException in SAXDOMParser");
		}
	    }
	}
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
        page = page.addEvent(doc, SAXEvent.TEXT, sb.toString());
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

    // for streaming, a place to put top-level elements
    public void setOutputStream(SourceStream outputStream) {
	this.outputStream = outputStream;
    }

}
