package niagara.firehose;

import org.w3c.dom.*;
import java.io.*;
import java.net.*;
import java.util.*;

class XMLTempGenerator extends XMLFirehoseGen {
    private int m_iTempCurr;
    private Random m_rnd;
    private long m_iTime;
    private long m_cHourLast;
    private String m_stId;
    private long m_cMinuteOffset;
    private String stFile;

    public XMLTempGenerator(String fileName, boolean streaming, boolean prettyprint) {
	m_iTempCurr = 70;
	m_rnd = new Random();
	m_iTime = 0;
	m_cHourLast = -1;
	m_stId = new String("X11");
	m_cMinuteOffset = 0;
	stFile = fileName;
	useStreamingFormat = streaming;
	usePrettyPrint = prettyprint;
    }

    public String generateXMLString() {
	//The parameter contains the id for this sensor, and
	// the time offset.
	if (stFile.length() != 0) {
	    m_stId = stFile.substring(0, 3);
	    if (stFile.length() >= 3) {
		try {
		    m_cMinuteOffset =
			Long.parseLong(stFile.substring(3));
		} catch (NumberFormatException ex) {
		    ;
		}
	    } else
		m_stId = stFile;
	}

	//One second = 1 millisecond
	StringBuffer stbRet = new StringBuffer();
	long cHour = m_iTime / 60;
	long cMinute = m_iTime % 60;
	m_iTime++;

	if(!useStreamingFormat)
	    stbRet.append("<?xml version=\"1.0\"?>");

	stbRet.append("<TEMPDATA>" +
		      "<TEMPERATURE>" +
		      "<ID>" + m_stId + "</ID>" +
		      "<HOUR>" + String.valueOf(cHour) +
		      "<MINUTE>" + String.valueOf(cMinute) + "</MINUTE>" +
		      "<CURRTEMP>" + String.valueOf(m_iTempCurr) +
		      "</CURRTEMP>" +
		      "</HOUR>" +
		      "</TEMPERATURE>");

	//Update the temp
	m_iTempCurr += (m_rnd.nextInt(5) - 2);
	//Check if we need to send punctuation
	if (m_cHourLast != cHour) {
	    if (m_cHourLast != -1) {
		stbRet.append("\n<punct:TEMPERATURE>" +
			      "<ID>*</ID>" +
			      "<HOUR>" + m_cHourLast +
			      "<MINUTE>*</MINUTE>" +
			      "<CURRTEMP>*</CURRTEMP>" +
			      "</HOUR>"+
			      "</punct:TEMPERATURE>");
	    }
	    m_cHourLast = cHour;
	} else if (cMinute % 1 == 0) {
	    stbRet.append("\n<punct:TEMPERATURE>");
	    stbRet.append("<ID>*</ID>");
	    stbRet.append("<HOUR>" + cHour + "." +
			  "." + cMinute + "");
	    stbRet.append("<MINUTE>" + cMinute + "]</MINUTE>");
	    stbRet.append("<CURRTEMP>*</CURRTEMP>");
	    stbRet.append("</HOUR>");
	    stbRet.append("</punct:TEMPERATURE>");
	}

	stbRet.append("</TEMPDATA>");

	return stbRet.toString();
    }
}


