import junit.framework.*;

import niagara.ndom.*;
import org.w3c.dom.*;
import org.xml.sax.*;
import java.io.*;

public class TestNDOM extends TestCase {
    // An input stream for the file we parse
    FileInputStream fis;

    public TestNDOM(String name) {
        super(name);
    }

    protected void setUp() throws IOException {
        fis = new FileInputStream("numbers.xml");
    }

    protected DOMParser getXML4JParser() {
        DOMFactory.setImpl("xml4j");
        return DOMFactory.newParser();
    }

    protected DOMParser getXercesParser() {
        DOMFactory.setImpl("xerces");
        return DOMFactory.newParser();
    }

    public void checkParsing(DOMParser parser) 
        throws SAXException, IOException {
        parser.parse(new InputSource(fis));
        assert("Errors or warnings reported", 
               !(parser.hasErrors() || parser.hasWarnings()));

        Document doc = parser.getDocument();
        assert("Null document after parsing", doc != null);

        Element root = doc.getDocumentElement();
        assert("No document element found after parsing", root != null);

        assert("Wrong document element", root.getTagName().equals("PLAY"));
    }

    public void testXML4JParser() throws SAXException, IOException {
        checkParsing(getXML4JParser());
    }

    public void testXercesParser() throws SAXException, IOException {
        checkParsing(getXercesParser());
    }
    
    public static void main(String args[]) {
        junit.textui.TestRunner.run(new TestSuite(TestNDOM.class));
    }
}

