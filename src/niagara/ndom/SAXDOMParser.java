/**
 * $Id: SAXDOMParser.java,v 1.7 2002/04/06 02:14:55 vpapad Exp $
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
import niagara.ndom.DOMParser;

/**
 * <code>SAXDOMParser</code> constructs read-only SAXDOM documents
 * from a SAX source
 */

public class SAXDOMParser extends DefaultHandler implements DOMParser {
    private SAXParser parser;

    private DocumentImpl doc;
    private Page page;

    // XXX vpapad: To test SAXDOM table building performance
    private static final boolean producingOutput = true;

    // XXX vpapad: We could make this a container, but creating and
    // casting Integers back and forth is not a nice thing to do for
    // each DOM node in a document. A value of 1024 for the size of
    // open_nodes covers all documents I've seen!

    private int[] open_nodes = new int[1024]; 
    private int depth;

    // We use this string buffer to normalize consecutive text nodes
    private StringBuffer sb = new StringBuffer();

    private static final String newLine = "\n";

    // for streaming support - KT
    private SourceStream outputStream;

    public SAXDOMParser() {
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            parser = factory.newSAXParser();
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
        reset();
        parser.parse(is, this);
    }

    public void reset() {
        doc = null;
        hasErrors = hasWarnings = false;
        sb.setLength(0);
        depth = -1;
    }

    public Document getDocument() {
        return doc;
    }

    public boolean supportsStreaming() {
	return false;
    }

    // for streaming, a place to put top-level elements
    public void setOutputStream(SourceStream outputStream) {
	this.outputStream = outputStream;
    }

    public void setPage(Page page) {
	this.page = page;
    }


    // Event handling
    
    public void startDocument() throws SAXException {
        if (page == null || page.isFull()) {
            page = BufferManager.getFreePage();
            page.setParser(this);
        }

        // If we're streaming, ignore the enclosing document 
        if (NiagraServer.STREAM) return;

        page.addEvent(doc, SAXEvent.START_DOCUMENT, null);
        doc = new DocumentImpl(page, page.getLastOffset());
        
        depth = 0; 
        open_nodes[0] = -1;
    }

    public void endDocument() throws SAXException {
        // if we're streaming, ignore the enclosing document
        if (NiagraServer.STREAM) return;

        handleText();
        page.addEvent(doc, SAXEvent.END_DOCUMENT, null);

        if (--depth != -1) 
            throw new PEException("Unbalanced open nodes list.");
    }

    public void startElement(String namespaceURI, String localName, 
                             String qName, Attributes attrs) 
        throws SAXException {

        // ignore stream header
        if (NiagraServer.STREAM && qName.equals("niagara:stream"))
            return;

        handleText();

        // If we're streaming, and this is a top-level element
        // pretend we just received a start document event
	if(NiagraServer.STREAM && depth == -1) {
            page.addEvent(doc, SAXEvent.START_DOCUMENT, null);
            doc = new DocumentImpl(page, page.getLastOffset());

            depth = 0; 
            open_nodes[0] = -1;            
        }
            

        // XXX vpapad: not doing anything about namespaces yet
	page.addEvent(doc, SAXEvent.START_ELEMENT, qName);

        int current = page.getLastIndex(); 
        int previous = open_nodes[depth];

        if (previous != -1) // We have a previous sibling
            BufferManager.getPage(previous).
                setNextSibling(BufferManager.getOffset(previous), current);

        open_nodes[depth] = current; // This is now the current open node
        depth++; // ... we are one level deeper ...
        open_nodes[depth] = -1; // ... and there are no nodes yet in this level
 
        for (int i = 0; i < attrs.getLength(); i++) {
            page.addEvent(doc, SAXEvent.ATTR_NAME, attrs.getQName(i));
            page.addEvent(doc, SAXEvent.ATTR_VALUE, attrs.getValue(i));
        }
    }

    public void endElement(String namespaceURI, String localName, String qName)
        throws SAXException {
        // ignore stream header
        if (NiagraServer.STREAM && qName.equals("niagara:stream")) 
            return;

        handleText();
        depth--; // This level is finished;

        // The next_sibling pointer in END_ELEMENT points to the
        // beginning of the element
        page.addEvent(doc, SAXEvent.END_ELEMENT, null);
        page.setNextSibling(page.getLastOffset(), open_nodes[depth]);

        // if we're streaming, and this is a top-level element
        // pretend we just received an end document event
        if(NiagraServer.STREAM && depth == 0) {
            page.addEvent(doc, SAXEvent.END_DOCUMENT, null);

            if (outputStream !=  null) {
                // for now throw fatal errors on these exceptions, if
                // they happen, I'll have to figure out what the right
                // thing is to do - KT
                // I don't want to do a lot of work until I know this
                // actually works
                try {
                    if (producingOutput)
                        outputStream.put(doc);
                } catch (java.lang.InterruptedException ie) {
                throw new PEException("KT - InterruptedException in SAXDOMParser");
                } catch (niagara.utils.NullElementException ne) {
                    throw new PEException("KT - NullElementException in SAXDOMParser");
                } catch (niagara.utils.StreamPreviouslyClosedException spce) {
                    throw new PEException("KT - StreamPreviouslyClosedException in SAXDOMParser");
                }

                if (--depth != -1) 
                    throw new PEException("Unbalanced open nodes list.");
            }
        }

    }

    public void characters(char[] ch, int start,int length) 
        throws SAXException {
        // if we're streaming and we're outside a top-level element
        // do nothing
        if (NiagraServer.STREAM && depth <= 0) return;

        sb.append(ch, start, length);
    }
    
    public void handleText() {
        // if we're streaming and we're outside a top-level element
        // do nothing
        if (NiagraServer.STREAM && depth <= 0) return;

        int length = sb.length();
        if (length > 0) {
            // Special case for newlines
            if (length == 1 && sb.charAt(0) == '\n')
                page.addEvent(doc, SAXEvent.TEXT, newLine);
            else
                page.addEvent(doc, SAXEvent.TEXT, sb.toString());
            sb.setLength(0);

            int current = page.getLastIndex(); 
            int previous = open_nodes[depth];

            if (previous != -1) // We have a previous sibling
                BufferManager.getPage(previous).
                    setNextSibling(BufferManager.getOffset(previous), current);

            open_nodes[depth] = current; // This is now the current open node
        }
    }
    
    // XXX vpapad: not handling these yet
    public void startPrefixMapping(String prefix, String uri) 
        throws SAXException {
    }

    public void endPrefixMapping(String prefix, String uri) 
        throws SAXException {
    }

    public void processingInstruction(String target, String data) 
        throws SAXException {
    }

    // Error handling
    private boolean hasErrors, hasWarnings;

    public boolean hasWarnings() {
        return hasWarnings;
    }

    public boolean hasErrors() {
        return hasErrors;
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
