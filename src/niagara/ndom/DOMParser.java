/**
 * $Id: DOMParser.java,v 1.1 2001/07/17 03:36:36 vpapad Exp $
 *
 */

package niagara.ndom;

import org.w3c.dom.*;
import org.xml.sax.*;
import java.io.IOException;

/**
 * <code>DOMParser</code> is the interface implemented by all
 * Niagara DOM implementations.
 *
 * @author <a href="mailto:vpapad@cse.ogi.edu">Vassilis Papadimos</a>
 */
public interface DOMParser {

    /**
     * <code>parse</code> parses an XML document and produces
     * a DOM tree (that can be retrieved by <code>getDocument()</code>
     *
     * @param inputSource an <code>InputSource</code> value
     * @exception SAXException if a parsing error occurs
     * @exception IOException if an I/O error occurs
     */
    void parse(InputSource inputSource) throws SAXException, IOException;


    /**
     * If parsing was successful <code>getDocument</code> 
     * returns the DOM Document node.
     *
     * @return a <code>Document</code> value
     */
    Document getDocument();

    /**
     * <code>hasErrors</code> 
     *
     * @return <code>true</code> if any errors occured during parsing
     */
    boolean hasErrors();

    /**
     * <code>hasWarnings</code> 
     *
     * @return <code>true</code> if any warnings occured during parsing
     */
    boolean hasWarnings();
}

