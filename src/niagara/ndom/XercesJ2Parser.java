package niagara.ndom;

import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * <code>XercesJParser</code> is a wrapper for
 * org.apache.xerces.parsers.DOMParser;
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
			System.err.println("Unable to create XercesJ2Parser "
					+ pce.getMessage());
		}
	}

	public void parse(org.xml.sax.InputSource is) throws SAXException,
			IOException {
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

	/*
	 * @see niagara.ndom.DOMParser#getErrorStrings()
	 */
	public String getErrorStrings() {
		return sh.getErrorStrings();
	}

	/*
	 * @see niagara.ndom.DOMParser#getWarningStrings()
	 */
	public String getWarningStrings() {
		return sh.getWarningStrings();
	}
}

class SimpleHandler2 extends DefaultHandler {
	private boolean hasErrors, hasWarnings;
	private StringBuffer errors;
	private StringBuffer warnings;

	public SimpleHandler2() {
		errors = new StringBuffer();
		warnings = new StringBuffer();
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
		errors.setLength(0);
		errors.setLength(0);
	}

	public void error(SAXParseException e) {
		hasErrors = true;
		errors.append(e.getMessage()).append("\n");
	}

	public void warning(SAXParseException e) {
		hasWarnings = true;
		warnings.append(e.getMessage()).append("\n");
	}

	public String getErrorStrings() {
		return errors.toString();
	}

	public String getWarningStrings() {
		return warnings.toString();
	}
}
