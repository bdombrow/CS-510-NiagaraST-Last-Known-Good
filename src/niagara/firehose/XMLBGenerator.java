package niagara.firehose;

import java.io.*;
import java.net.*;


class XMLBGenerator extends XMLFirehoseGen {
    static String m_STHTTPXMLBROOT = new String("gamedata/");
    static String m_STREALXMLBROOT =
	new String("/disk/hopper/projects/disc/niagara/" + m_STHTTPXMLBROOT);
    //	m_STHTTPXMLBROOT;
    static File m_fileXMLBRoot = new File(m_STREALXMLBROOT);
    static BufferedReader m_brdr;
    static int m_iXMLBFile = 0;

    public XMLBGenerator(int numTLElts, boolean streaming, boolean prettyprint) {
	this.numTLElts = numTLElts;
	useStreamingFormat = streaming;
	usePrettyPrint = prettyprint;
    }

    private void setupReader() {
	ExtFilenameFilter eff = new ExtFilenameFilter(".xml");
	String[] rgstFiles = m_fileXMLBRoot.list((FilenameFilter) eff);
	String stFileOut = new String(m_STREALXMLBROOT
				      + rgstFiles[m_iXMLBFile]);

	// send the file
	try {
	    m_brdr = new BufferedReader(new FileReader(stFileOut));
	} catch (java.io.FileNotFoundException ex) {
	    m_brdr = null;
	}

	m_iXMLBFile = (m_iXMLBFile + 1) % rgstFiles.length;
    }

    // was getXMLBData()
    public String generateXMLString() {

	int iElement = -1;
	String stEnd = null;
	StringBuffer stb = new StringBuffer();
	String stLine = null;
	boolean fDoctype = false;
	
	try {
	    if (m_brdr == null)
		setupReader();

	    if(!useStreamingFormat) {
		stb.append("<?xml version = \"1.0\"?>\n");
		if (fDoctype == true) {
		    stb.append("<!DOCTYPE GAMEDATA SYSTEM \""
			       + m_STREALXMLBROOT
			       + "gamedata.dtd\">\n");
		}
	    }
	    stb.append("<GAMEDATA>");
	    if(usePrettyPrint)
		stb.append("\n");
	    for (int iDoc = 0; iDoc < numTLElts; iDoc++) {
		iElement = -1;
		while (iElement == -1) {
		    stLine = m_brdr.readLine();
		    if (stLine == null)
			setupReader();
		    else {
			if (stLine.indexOf("<PLAY>") != -1) {
			    stEnd = new String("</PLAY>");
			    iElement = 0;
			} else if (stLine.indexOf("<GAME_START>") != -1) {
			    stEnd = new String("</GAME_START>");
			    iElement = 1;
			} else if (stLine.indexOf("<GAME_END>") != -1) {
			    stEnd = new String("</GAME_END>");
			    iElement = 2;
			}
		    }
		}
		
		while (stLine.indexOf(stEnd) == -1) {
		    stb.append(stLine);
		    if(usePrettyPrint)
			stb.append('\n');
		    stLine = m_brdr.readLine();
		}
		
		//Get the end token
		stb.append(stLine);
		if(usePrettyPrint)
		    stb.append('\n');
	    }
	    stb.append("</GAMEDATA>");
	    if(usePrettyPrint)
		stb.append('\n');

	} catch (IOException ioe) {
	    ioe.printStackTrace();
	    System.out.println("Guilty XML: ");
	    System.out.println("\t" + stb.toString());
	}
	return stb.toString();
    }
}
