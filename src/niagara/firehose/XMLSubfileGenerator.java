package niagara.firehose;

import java.util.Vector;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.AttributeList;
import org.xml.sax.HandlerBase;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

class XMLSubfileGenerator extends XMLFirehoseGen {

    private String stFile;
    private SubfileHandler m_sh;

    public XMLSubfileGenerator(String fileName, boolean streaming, boolean prettyprint) {
	stFile = fileName;
	useStreamingFormat = streaming;
	usePrettyPrint = prettyprint;
    }

    public String generateXMLString() {

	// KT - WARNING - need to handle useStreamingFormat in this function
	// but I don't see how to do it. if useStreamingFormat is true,
	// no <?xml... header or doctype declaration should be included
	// in the string returned from this function

	if (m_sh == null) 
	    m_sh = new SubfileHandler();
	
	return m_sh.getNextDoc(stFile);
    }
}

class SubfileHandler extends HandlerBase {
    public String getNextDoc(String stFile) {
	//If we haven't done so already, parse the XML file
	if (m_vctDocs == null) {
	    try {
		SAXParserFactory factory = SAXParserFactory.newInstance();
		SAXParser parser = factory.newSAXParser();
		parser.parse(stFile, this);
	    }
	    catch (Exception e) {
		e.toString();
	    }
	}

	//Now, return whichever subdocument we're on.
	String stRet;
	if (m_vctDocs.size() != 0) {
	    stRet = (String) m_vctDocs.get(m_iDoc);
	    m_iDoc = (m_iDoc + 1) % m_vctDocs.size();
	} else {
	    stRet = new String("");
	}
	return stRet;
    }

 /** Start document. */
    public void startDocument() {
	m_vctDocs = new Vector();
	m_iDoc = 0;
    } // startDocument()

 /** Start element. */
    public void startElement(String stName, AttributeList attrs) {
	if (m_stDoc == null) {
	    m_stDoc = new String(stName);
	} else if (m_stSubDocTitle == null) {
	    m_stSubDocTitle = new String(stName);
	    m_stbSubDoc = new StringBuffer("<?xml version = \"1.0\"?>\n" + 
					   "<" + stName + ">\n");
	} else if (m_stbSubDoc != null) {
	    m_stbSubDoc.append("<" + stName + "> ");
	}
    } // startElement(String,AttributeList)

    /** Characters. */
    public void characters(char rgch[], int iStart, int cch) {
	String st = new String(rgch, iStart, cch);
	st = st.trim();
	m_stbSubDoc.append(st);
	m_stbSubDoc.append(" ");
    } // characters(char[],int,int);

    public void endElement(String stName) {
	if (stName.compareToIgnoreCase(m_stDoc) == 0) {
	    //Cool, we're done.
	    //Nothing really we need to do.
	} else if (stName.compareToIgnoreCase(m_stSubDocTitle) == 0) {
	    m_stbSubDoc.append("</" + stName + ">\n");
	    m_vctDocs.add(m_stbSubDoc.toString());
	    m_stSubDocTitle = null;
	} else {
	    m_stbSubDoc.append("</" + stName + ">\n");
	}
    }

    public void endDocument() {
	;
    }

    /** Ignorable whitespace. */
    public void ignorableWhitespace(char ch[], int start, int length) {
	;
    } // ignorableWhitespace(char[],int,int);

    //
    // ErrorHandler methods
    //
    /** Warning. */
    public void warning(SAXParseException ex) {
        System.err.println("[Warning] "+
                           getLocationString(ex)+": "+
                           ex.getMessage());
    }

    /** Error. */
    public void error(SAXParseException ex) {
        System.err.println("[Error] "+
                           getLocationString(ex)+": "+
                           ex.getMessage());
    }

    /** Fatal error. */
    public void fatalError(SAXParseException ex) throws SAXException {
        System.err.println("[Fatal Error] "+
                           getLocationString(ex)+": "+
                           ex.getMessage());
        throw ex;
    }

    /** Returns a string of the location. */
    private String getLocationString(SAXParseException ex) {
        StringBuffer str = new StringBuffer();

        String systemId = ex.getSystemId();
        if (systemId != null) {
            int index = systemId.lastIndexOf('/');
            if (index != -1) 
                systemId = systemId.substring(index + 1);
            str.append(systemId);
        }
        str.append(':');
        str.append(ex.getLineNumber());
        str.append(':');
        str.append(ex.getColumnNumber());

        return str.toString();

    } // getLocationString(SAXParseException):String

    private String m_stDoc;
    private String m_stSubDocTitle;
    private Vector m_vctDocs;
    private StringBuffer m_stbSubDoc;
    private int m_iDoc;
}
