package niagara.firehose;

import java.io.*;
import java.net.*;
import java.util.*;
import com.ibm.XMLGenerator.*;
import net.sourceforge.jpcap.capture.*;
import net.sourceforge.jpcap.net.*;
import net.sourceforge.jpcap.util.*;

public class XMLPacketGenerator extends XMLFirehoseGen {
    private static PacketHandler m_ph = null;
    private static boolean m_fLive;
    private String m_stFile;
    private boolean m_fEOF = false;
    private StringBuffer m_stb = null;
    String tab = "";
    String tab2 = "";
    String tab3 = "";
    String nl = "";

    public XMLPacketGenerator(String stFile, boolean fLive,
			      int numTLElts, boolean streaming,
			      boolean prettyPrint) {
	m_ph = new PacketHandler();
	m_fLive = fLive;
	m_stFile = stFile;
	this.numTLElts = numTLElts;
	useStreamingFormat = streaming;
	usePrettyPrint = prettyPrint;

	if(usePrettyPrint) {
	    tab = "   ";
	    tab2 = tab + tab;
	    tab3 = tab + tab + tab;
	    nl = "\n";
	}
    }

    public String generateXMLString() {
	if (m_stb == null) {
	    m_stb = new StringBuffer(1000*numTLElts);
	}
	m_stb.append("<PS>");
	m_stb.append(nl);
	m_ph.init(m_stFile, m_fLive, usePrettyPrint);
	for (int iDoc = 0; m_fEOF == false && iDoc < numTLElts; iDoc++) {
	    m_stb.append(tab);
	    m_stb.append("<P>");
	    m_stb.append(nl);

	    m_ph.getPacket(m_fLive, m_stb);

	    m_stb.append(tab);
	    m_stb.append("</P>");
	    m_stb.append(nl);
	    m_fEOF = m_ph.getEOF();
	}
	m_stb.append("</PS>");
	m_stb.append(nl);

	String stOut = m_stb.toString();
	m_stb.setLength(0);
	return stOut;
    }

    public boolean getEOF() { return m_fEOF; }
}

class PacketInit extends Thread {
   PacketHandler m_ph;
   private static String m_device;

   private static final int INFINITE = -1;
   private static final int PACKET_COUNT = INFINITE;
   private static final int SNAPLEN = 1024;
   // BPF filter for capturing any packet
   private static String m_stFilter = "tcp port 80";

   PacketInit(PacketHandler ph, String filter) {
      m_ph = ph;
      if (filter != null)
	  m_stFilter = filter;
   }

   public void run() {
      PacketCapture pcap;

      try {
         // Step 1:  Instantiate Capturing Engine
         pcap = new PacketCapture();

         // Step 2:  Check for devices
         m_device = pcap.findDevice();

         // Step 3:  Open Device for Capturing (requires root)
         pcap.open(m_device, SNAPLEN, true, 20000);

         // Step 4:  Add a BPF Filter (see tcpdump documentation)
         pcap.setFilter(m_stFilter, true);

         // Step 5:  Register a Listener for jpcap Packets
         pcap.addPacketListener(new PacketHandler());

         // Step 6:  Capture Data (max. PACKET_COUNT packets)
         pcap.capture(PACKET_COUNT);

      } catch (Exception ex) {
         System.out.println("Failed to init: " + ex.toString());
      }
   }
}

class PacketHandler implements PacketListener
{
    class ByteArrayHandler {
	private byte[][] m_rgrgb;
	private int[] m_rgc;
	private int m_iNext = 0;
	private int m_crg = 0;

	public ByteArrayHandler(int cRg) {
	    m_crg = cRg;
	    m_rgrgb = new byte[cRg][];
	    m_rgc = new int[cRg];
	    for (int i=0; i<cRg; i++) {
		m_rgc[i] = 0;
	    }
	}

	public byte[] newArray(int cb) {
	    int i;

	    //see if we already have this allocated
	    for (i=0; i < m_crg; i++) {
		if (cb == m_rgc[i])
		    break;
	    }

	    if (i == m_crg) {
		m_rgrgb[m_iNext] = new byte[cb];
		m_rgc[m_iNext] = cb;
		i = m_iNext;
		m_iNext = (m_iNext+1) % m_crg;
	    }

	    return m_rgrgb[i];
	}
    }

    private static PacketInit m_pi = null;
    private static int m_counter = 0;
    static private Vector m_vctPackets = new Vector();
    static private InputStream m_tcpdump = null;
    private ArrayHelper array = new ArrayHelper();
    private String m_stFile;
    private int m_cbRead = 0;
    private ByteReader m_br = new ByteReader();
    private byte[] m_rgbPacketLength = new byte[4];
    private ByteArrayHandler m_bah = new ByteArrayHandler(10);
    private static int m_rgbitIP = 0xFFFF;   //all IP fields
    private static int m_rgbitTCP = 0x0001 | 0x0002;  //only Src & Dst Port
    private static int m_rgbitHTTP = 0xFFFF; //all HTTP fields
    private static int m_nSessionGap = -1;
    private static Hashtable m_htSession = new Hashtable();
    private static long m_idSession = 0;
    private HTTPFields m_hf = new HTTPFields();
    boolean m_fEOF = false;
    String tab = "";
    String tab2 = "";
    String tab3 = "";
    String nl = "";

    public void init(String stFile, boolean fLive, boolean usePrettyPrint) {
	m_stFile = stFile;
	m_cbRead = 0;

	if(usePrettyPrint) {
	    tab = "   ";
	    tab2 = tab + tab;
	    tab3 = tab + tab + tab;
	    nl = "\n";
	}

	if (m_pi == null && fLive) {
	    StringTokenizer stok = new StringTokenizer(stFile, ":");
	    StringBuffer stb = new StringBuffer();
	    while (stok.hasMoreTokens()) {
		stb.append((String) stok.nextElement());
		stb.append(" ");
	    }
	    m_pi = new PacketInit(this, stb.toString());
	    m_pi.start();
        }
    }

    public int getBytesRead() {
	return m_cbRead;
    }

    public boolean getEOF() {
	return m_fEOF;
    }

    public void getPacket(boolean fLive, StringBuffer stb) {
	TCPPacket tcppkt;

	try{
	    if (fLive) {
		//Wait until there are packets available
		synchronized(m_vctPackets) {
		    if (m_vctPackets.size() == 0)
			m_vctPackets.wait();
		    tcppkt = (TCPPacket) m_vctPackets.elementAt(0);
		    m_vctPackets.removeElementAt(0);
		}

		long ts = genTimeStampElement(fLive, stb);

		getTCPPacket(fLive, tcppkt, stb, ts);
	    } else
		HTTP_Parser(stb);

	} catch (IOException e) { ;
	} catch (InterruptedException ex) {;}
    }

    class SessionProps {
	public long m_idSession;
	public long m_tsLast;
    }

    private void getTCPPacket(boolean fLive, TCPPacket pkt, 
			      StringBuffer stb, long ts)
	throws java.io.IOException {
	//Define a session by the source IP and source port.
	String stSIP = null;
	long idSession = m_idSession;

	if (m_nSessionGap != -1) {
	    if (stSIP == null)
		stSIP = pkt.getSourceAddress();
	    m_idSession++;

	    //Determine the session id
	    SessionProps prp = (SessionProps) m_htSession.get(stSIP);
	    if (prp != null) {
		if (ts - prp.m_tsLast < m_nSessionGap) {
		    idSession = prp.m_idSession;
		    m_idSession--;
		}
	    } else
		prp = new SessionProps();

	    prp.m_idSession = idSession;
	    prp.m_tsLast = ts;
	    m_htSession.put(stSIP, prp);
	    genElem("SES", idSession, stb, tab2);
	}

	//Get the IP information
	if (m_rgbitIP != 0) {
	    stb.append(tab2);
	    stb.append("<IP>");
	    stb.append(nl);
	    if ((m_rgbitIP & 0x0001) != 0) {
		if (stSIP == null)
		    genElem("SI", pkt.getSourceAddress(), stb, tab3);
		else
		    genElem("SI", stSIP, stb, tab3);
	    }
	    if ((m_rgbitIP & 0x0002) != 0)
		genElem("DI", pkt.getDestinationAddress(), stb, tab3);
	    if ((m_rgbitIP & 0x0004) != 0)
		genElem("ID", pkt.getId(), stb, tab3);
	    if ((m_rgbitIP & 0x0008) != 0)
		genElem("FR", pkt.getFragmentOffset(), stb, tab3);
	    //if (pkt.getFragmentFlags() & 0x0001)
	    //Punctuation: No more packets with this ID.
	    stb.append(tab2);
	    stb.append("</IP>");
	    stb.append(nl);
	}

	//Get the TCP information
	if (m_rgbitTCP != 0) {
	    stb.append(tab2);
	    stb.append("<TCP>");
	    stb.append(nl);
	    if ((m_rgbitTCP & 0x0001) != 0)
		genElem("SP", pkt.getSourcePort(), stb, tab3);
	    if ((m_rgbitTCP & 0x0002) != 0)
		genElem("DP", pkt.getDestinationPort(), stb, tab3);
	    if ((m_rgbitTCP & 0x0004) != 0)
		genElem("SQ", pkt.getSequenceNumber(), stb, tab3);
	    if ((m_rgbitTCP & 0x0008) != 0)
		genElem("AN", pkt.getAcknowledgmentNumber(), stb, tab3);
	    if ((m_rgbitTCP & 0x0010) != 0)
		genElem("WS", pkt.getWindowSize(), stb, tab3);
	    stb.append(tab2);
	    stb.append("</TCP>");
	    stb.append(nl);
	}

	if (m_rgbitHTTP != 0) {
	    // retrieve the data from the packet, and then
	    // read line by line to parse for HTTP header values
	    m_br.setByteArray(pkt.getData());
	    if (m_br.lineContains("HTTP", 3)) {
		String method = m_br.readLine();
		genHTTPElement(method, stb);
	    }
	}
    }

    public void packetArrived(Packet packet) {
	m_counter++;
	StringBuffer stbOut = new StringBuffer(120);

	// cast packet onto TCPPacket to extract source and
	//  destination TCP addresses.
        TCPPacket tcppkt = (TCPPacket) packet;
	synchronized (m_vctPackets) {
	    m_vctPackets.add(tcppkt);
	    //Let a waiting request know that there is some data
	    m_vctPackets.notify();
	}
    }

    private void openTraceFile(String stFile)
	throws FileNotFoundException, IOException {

	//Find any tcpdump parameters. If there are none, use
	// the buffered file instead of tcpdump, since it is faster
	boolean fTCPDump = false;
	StringBuffer cmdarray = null;
	StringTokenizer stok = new StringTokenizer(stFile, ":");
	String stRead = (String) stok.nextElement();
	int cTilde = 0;
	if (stok.countTokens() > 1) {
	    cmdarray = new StringBuffer();
	    cmdarray.append("/usr/sbin/tcpdump -r ");
	    cmdarray.append(stRead);
	    cmdarray.append(" -s 0 -w -");
	}

	while (stok.hasMoreTokens()) {
	    String stTok = (String) stok.nextElement();
	    if (stTok.startsWith("SGAP")) {
		m_nSessionGap = Integer.parseInt(stTok.substring(4));
	    } else if (stTok.startsWith("IP")) {
		m_rgbitIP = 0;
		stTok = (String) stok.nextElement();
		while (stTok.equals("IP") == false && stok.hasMoreTokens()) {
		    if (stTok.equals("SI"))
			m_rgbitIP |= 0x01;
		    else if (stTok.equals("DI"))
			m_rgbitIP |= 0x02;
		    else if (stTok.equals("ID"))
			m_rgbitIP |= 0x04;
		    else if (stTok.equals("FR"))
			m_rgbitIP |= 0x08;

		    stTok = (String) stok.nextElement();
		}
	    } else if (stTok.startsWith("TCP")) {
		m_rgbitTCP = 0;
		stTok = (String) stok.nextElement();
		while (stTok.equals("TCP") == false && stok.hasMoreTokens()) {
		    if (stTok.equals("SP"))
			m_rgbitTCP |= 0x01;
		    else if (stTok.equals("DP"))
			m_rgbitTCP |= 0x02;
		    else if (stTok.equals("SQ"))
			m_rgbitTCP |= 0x04;
		    else if (stTok.equals("AN"))
			m_rgbitTCP |= 0x08;
		    else if (stTok.equals("WS"))
			m_rgbitTCP |= 0x10;

		    stTok = (String) stok.nextElement();
		}
	    } else if (stTok.startsWith("HTTP")) {
		m_rgbitHTTP = 0;
		stTok = (String) stok.nextElement();
		while (stTok.equals("HTTP") == false && stok.hasMoreTokens()) {
		    if (stTok.equals("MD"))
			m_rgbitHTTP |= 0x1000;
		    else if (stTok.equals("URL"))
			m_rgbitHTTP |= 0x2000;
		    else if (stTok.equals("AL"))
			m_rgbitHTTP |= 0x0001;
		    else if (stTok.equals("CC"))
			m_rgbitHTTP |= 0x0002;
		    else if (stTok.equals("CN"))
			m_rgbitHTTP |= 0x0004;
		    else if (stTok.equals("DT"))
			m_rgbitHTTP |= 0x0008;
		    else if (stTok.equals("V"))
			m_rgbitHTTP |= 0x0010;
		    else if (stTok.equals("A"))
			m_rgbitHTTP |= 0x0020;
		    else if (stTok.equals("H"))
			m_rgbitHTTP |= 0x0040;
		    else if (stTok.equals("PA"))
			m_rgbitHTTP |= 0x0080;
		    else if (stTok.equals("R"))
			m_rgbitHTTP |= 0x0100;

		    stTok = (String) stok.nextElement();
		}
	    } else {
		fTCPDump = true;
		cmdarray.append(" ");
		//HACK to get parenthesis to TCPdump
		if (stTok.equals("~")) {
		    if (cTilde % 2 == 0)
			cmdarray.append("(");
		    else
			cmdarray.append(")");
		    cTilde++;
		} else
		    cmdarray.append(stTok);
	    }
	}

	if (fTCPDump) {
	    Runtime rt = Runtime.getRuntime();
	    System.out.print("Calling : ");
	    System.out.println(cmdarray.toString());
	    Process pr = rt.exec(cmdarray.toString());
	    m_tcpdump = new FirehoseBufferedInputStream(pr.getInputStream());
	} else
	    m_tcpdump =
		new FirehoseBufferedInputStream(new FileInputStream(stRead),
						6144);

	m_fEOF = false;
	completeSkip(m_tcpdump, 24);
    }

    public void closeTraceFile() {
	try {
	    if (m_tcpdump != null)
		m_tcpdump.close();
	    m_fEOF = true;
	    m_tcpdump=null;
	} catch (IOException e) {
	    ;
	}
    }

    byte[] temp = new byte[4];
    public String HTTP_Parser (StringBuffer stbOut) {
	m_counter = 0;
	long i = 0;
	int len = 0;
	byte[] rgbPacket = null;
        int l;
	try {
	    if (m_tcpdump == null)
		openTraceFile(m_stFile);

	    long ts = genTimeStampElement(false, stbOut);

	    i = completeRead(m_tcpdump, temp, 0, temp.length);
	    if (i == -1) {
		closeTraceFile();
		return "";
	    }
	    m_counter++;

	    for(l=0;l<4;l++) m_rgbPacketLength[l] = temp[3-l];


	    len = array.extractInteger(m_rgbPacketLength, 0, 4);

	    if (completeSkip(m_tcpdump, 4) == -1) {
               m_tcpdump.close();
               m_fEOF = true;
	    }

	    m_cbRead += len;
	    rgbPacket = m_bah.newArray(len);

	    if (completeRead(m_tcpdump, rgbPacket, 0, len) == -1) {
		closeTraceFile();
		return "";
	    }

	    EthernetPacket eth_packet = new EthernetPacket(14, rgbPacket);
	    if(eth_packet.getProtocol() == EthernetProtocols.IP) {
		TCPPacket packet = new TCPPacket  (14, rgbPacket);
		if (packet.getProtocol() == IPProtocols.TCP) {
		    getTCPPacket(false, packet, stbOut, ts);
		}
	    }
        } catch (IOException e) {
	    System.out.println("Read Exception : " + e.getMessage());
	    closeTraceFile();
        } catch (ArrayIndexOutOfBoundsException e) {
	    System.out.println("Read Exception : " + e.getMessage());
	    e.printStackTrace();
	    System.out.println("packet:");
	    for (int iTmp=0; iTmp<rgbPacket.length; iTmp++) {
		System.out.print(Integer.toHexString(rgbPacket[iTmp]));
		System.out.print(" ");
		if ((iTmp % 15) == 0)
		    System.out.println();
	    }
	    System.out.println("cbPacket : " + rgbPacket.length);
	    closeTraceFile();
	}

	return stbOut.toString();
    }

    byte[] seconds = new byte[4];
    byte[] micro = new byte[4];
    private long genTimeStampElement(boolean fLive, StringBuffer stbOut) {
	long sec = 0;
	int mic = 0;

	// following lines are added to extract timestamp.
	if (fLive) {
	    long ms = System.currentTimeMillis();
	    sec = ms/1000;
	    mic = (int) (ms % 1000) * 1000;
	} else {
	    try {
		if (completeRead(m_tcpdump, temp, 0, temp.length) == -1) {
		    closeTraceFile();
		    return 0;
		}

		int l;
		for(l=0;l<4;l++) seconds[l] = temp[3-l];

		if (completeRead(m_tcpdump, temp, 0, temp.length) == -1) {
		    closeTraceFile();
		    return 0;
		}

		for(l=0;l<4;l++) micro[l] = temp[3-l];

		sec = array.extractLong(seconds, 0, 4);
		mic = array.extractInteger(micro, 0, 4);

		// code for timestamps ends here...
	    } catch (IOException ex) {;}
	}

	stbOut.append(tab2);
	stbOut.append("<TS>");
	stbOut.append(sec);
	stbOut.append(".");
	stbOut.append(mic);
	stbOut.append("</TS>");
	stbOut.append(nl);

	return ((sec*1000) + mic);
    }

    private void genHTTPElement(String stRequest, StringBuffer stbOut)
	throws java.io.IOException {
	StringTokenizer stok = new StringTokenizer(stRequest);

	stbOut.append(tab2);
	stbOut.append("<HTTP>");
	stbOut.append(nl);
        //First token is the method
	if ((m_rgbitHTTP & 0x1000) != 0)
	    genElem("MD", stok.nextToken(), stbOut, tab3);
	//Second token is the URL
	if ((m_rgbitHTTP & 0x2000) != 0)
	    genElem("URL", stok.nextToken(), stbOut, tab3);

	while (m_br.nextLine()) {
	    String stValue = null;
	    String stTag = null;
	    int iTag = m_hf.findHTTPField(m_br);
	    if (iTag != -1 && ((int) Math.pow(2, iTag) & m_rgbitHTTP) != 0) {
		stTag = m_hf.getTag(iTag);
		stValue = m_br.readLine(m_hf.getValue());
	    }

	    if (stTag != null && stValue != null)
		genElem(stTag, stValue, stbOut, tab3);
	}

	stbOut.append(tab2);
	stbOut.append("</HTTP>");
	stbOut.append(nl);
    }

    private void genElem(String stTag, long nValue, StringBuffer stb,
			 String tab) {
	stb.append(tab);
	stb.append("<");
	stb.append(stTag);
	stb.append(">");

	stb.append(nValue);

	stb.append("</");
	stb.append(stTag);
	stb.append(">");
	stb.append(nl);
    }

    private void genElem(String stTag, String stValue, StringBuffer stb,
			 String tab) {
	int i, iChar;
	char ch;

	stb.append(tab);
	stb.append("<");
	stb.append(stTag);
	stb.append(">");

	//Need to change '&' to '&amp;', and remove invalid characters
	for (i = 0; i < stValue.length(); i++) {
	    for (iChar = i; iChar < stValue.length(); iChar++) {
		ch = stValue.charAt(iChar);
		if (ch == '&') {
		    if (iChar > i)
			stb.append(stValue.substring(i, iChar-1));
		    stb.append("&amp;");
		    break;
		} else if (ch == '<') {
		    if (iChar > i)
			stb.append(stValue.substring(i, iChar-1));
		    stb.append("&lt;");
		    break;
		} else if (ch == '>') {
		    if (iChar > i)
			stb.append(stValue.substring(i, iChar-1));
		    stb.append("&gt;");
		    break;
		} else if (ch < 0x9 || ch == 0xB || ch == 0xC ||
			   (ch > 0xD && ch < 0x20) ||
			   (ch > 0x7F && ch <= 0xFF) ||
			   (ch > 0xD7FF && ch < 0xE000) || ch > 0xFFFD) {
		    if (iChar > i)
			stb.append(stTag + ":" + 
				   stValue.substring(i, iChar-1));
		    stb.append(" 0x");
		    stb.append(Integer.toHexString(ch));
		    stb.append(" ");
		    break;
		}
	    }
	    if (iChar == stValue.length()) //finish the job
		stb.append(stValue.substring(i));
	    i = iChar;
	}

	stb.append("</");
	stb.append(stTag);
	stb.append(">");
	stb.append(nl);
    }

    private int completeRead(InputStream is, byte[] rgb, int iOffset,
			     int cbTotal)
	throws java.io.IOException {
	int iRead = 0;

	if (m_fEOF)
	    iRead = -1;

	while (iRead != -1 && cbTotal > 0) {
	    iRead = is.read(rgb, iOffset, cbTotal);
	    cbTotal -= iRead;
	    iOffset += iRead;
	}

	if (iRead == -1)
	    System.out.println("EOF occurred");
	return iRead;
    }

    private long completeSkip(InputStream is, int cbTotal)
	throws java.io.IOException {
	long iRead = is.skip(cbTotal);
	long cbSkip = iRead;

	if (m_fEOF)
	    iRead = -1;
	while (iRead != -1 && cbSkip < cbTotal) {
	    iRead = is.skip(cbTotal-cbSkip);
	    if (iRead == 0)
		iRead = (is.read() != -1) ? 1 : -1;
	    cbSkip += iRead;
	}

	return (iRead == -1) ? -1 : cbSkip;
    }
}

class HTTPFields {
    private int m_iValue;

    private String[][] rgstFields = {{"Accept-Language", "AL"},
				     {"Cache-Control", "CC"},
				     {"Connection", "CN"},
				     {"Date", "DT"},
				     {"Via", "V"},
				     {"Authorization", "A"},
				     {"Host", "H"},
				     {"Proxy-Authorization","PA"},
				     {"Referer","R"}};

    public String getTag(int iTag) {
	return rgstFields[iTag][1];
    }

    public int getValue() {
	//Offset it by two, to get to the first non-whitespace
	return m_iValue + 2; 
    }

    public int findHTTPField(ByteReader br) {
	boolean fDone = false;
	int iFound = -1;
	int iState = 0;
	char ch;
	m_iValue = br.getOffset();

	for (ch = (char) br.getByteAt(m_iValue);
	     fDone == false && ch != '\n' && ch != 0;
	     ch = (char) br.getByteAt(++m_iValue)) {
	    switch (ch) {
	    case '-':
		if (iState == 106) iState = 107;
		else if (iState == 305) iState = 306;
		else if (iState == 705) iState = 706;
		else fDone = true;
		break;
	    case 'A':
		if (iState == 0) iState = 101;
		else if (iState == 706) iState = 707;
		else fDone = true;
		break;
	    case 'a':
		if (iState == 108) iState = 109;
		else if (iState == 112) iState = 113;
		else if (iState == 208) iState = 209;
		else if (iState == 301) iState = 302;
		else if (iState == 501) iState = 502;
		else if (iState == 714) iState = 715;
		else if (iState == 902) {
		    //Via
		    fDone = true;
		    iFound = 4;
		}
		else fDone = true;
		break;
	    case 'C':
		if (iState == 0) iState = 301;
		else if (iState == 306) iState = 307;
		else fDone = true;
		break;
	    case 'c':
		if (iState == 101) iState = 102;
		else if (iState == 102) iState = 103;
		else if (iState == 302) iState = 303;
		else if (iState == 405) iState = 406;
		else fDone = true;
		break;
	    case 'D':
		if (iState == 0) iState = 501;
		else fDone = true;
		break;
	    case 'e':
		if (iState == 103) iState = 104;
		else if (iState == 114) {
		    //Accept-Language
		    fDone = true;
		    iFound = 0;//0th item in rgstFields
		}
		else if (iState == 304) iState = 305;
		else if (iState == 404) iState = 405;
		else if (iState == 503) {
		    //Date
		    fDone = true;
		    iFound = 3;
		}
		else if (iState == 801) iState = 802;
		else if (iState == 803) iState = 804;
		else if (iState == 805) iState = 806;
		else fDone = true;
		break;
	    case 'f':
		if (iState == 802) iState = 803;
		else fDone = true;
		break;
	    case 'g':
		if (iState == 110) iState = 111;
		else if (iState == 113) iState = 114;
		else fDone = true;
		break;
	    case 'H':
		if (iState == 0) iState = 601;
		else fDone = true;
		break;
	    case 'h':
		if (iState == 203) iState = 204;
		else if (iState == 303) iState = 304;
		else if (iState == 709) iState = 710;
		else fDone = true;
		break;
	    case 'i':
		if (iState == 206) iState = 207;
		else if (iState == 210) iState = 211;
		else if (iState == 407) iState = 408;
		else if (iState == 712) iState = 713;
		else if (iState == 716) iState = 717;
		else if (iState == 901) iState = 902;
		else fDone = true;
		break;
	    case 'L':
		if (iState == 107) iState = 108;
		else fDone = true;
		break;
	    case 'l':
		if (iState == 312) {
		    //Cache-control
		    fDone = true;
		    iFound = 1;//1st item in rgstFields
		}
		else fDone = true;
		break;
	    case 'n':
		if (iState == 109) iState = 110;
		else if (iState == 212) {
		    fDone = true;
		    iFound = 5;//5th item in rstFields
		}
		else if (iState == 308) iState = 309;
		else if (iState == 402) iState = 403;
		else if (iState == 403) iState = 404;
		else if (iState == 409) {
		    //Connection
		    fDone = true;
		    iFound = 2;
		}
		else if (iState == 718) {
		    //Proxy-Authorization
		    fDone = true;
		    iFound = 7;
		}
		else fDone=true;
		break;
	    case 'o':
		if (iState == 204) iState = 205;
		else if (iState == 211) iState = 212;
		else if (iState == 301) iState = 402;
		else if (iState == 307) iState = 308;
		else if (iState == 311) iState = 312;
		else if (iState == 408) iState = 409;
		else if (iState == 601) iState = 602;
		else if (iState == 702) iState = 703;
		else if (iState == 710) iState = 711;
		else if (iState == 717) iState = 718;
		else fDone = true;
		break;
	    case 'P':
		if (iState == 0) iState = 701;
		else fDone = true;
		break;
	    case 'p':
		if (iState == 104) iState = 105;
		else fDone = true;
		break;
	    case 'R':
		if (iState == 0) iState = 801;
		else fDone = true;
		break;
	    case 'r':
		if (iState == 205) iState = 206;
		else if (iState == 310) iState = 311;
		else if (iState == 701) iState = 702;
		else if (iState == 711) iState = 712;
		else if (iState == 804) iState = 805;
		else if (iState == 806) {
		    //Referer
		    fDone = true;
		    iFound = 8;//8th item in rgstFields
		}
		else fDone = true;
		break;
	    case 's':
		if (iState == 602) iState = 603;
		else fDone = true;
		break;
	    case 't':
		if (iState == 105) iState = 106;
		else if (iState == 202) iState = 203;
		else if (iState == 209) iState = 210;
		else if (iState == 309) iState = 310;
		else if (iState == 406) iState = 407;
		else if (iState == 502) iState = 503;
		else if (iState == 603) {
		    //Host
		    fDone = true;
		    iFound = 6;//6th item in rgstFields
		}
		else if (iState == 708) iState = 709;
		else if (iState == 715) iState = 716;
		else fDone = true;
		break;
	    case 'u':
		if (iState == 101) iState = 202;
		else if (iState == 111) iState = 112;
		else if (iState == 707) iState = 708;
		else fDone = true;
		break;
	    case 'V':
		if (iState == 0) iState = 901;
		else fDone = true;
		break;
	    case 'x':
		if (iState == 703) iState = 704;
		else fDone = true;
		break;
	    case 'y':
		if (iState == 704) iState = 705;
		else fDone = true;
		break;
	    case 'z':
		if (iState == 207) iState = 208;
		else if (iState == 713) iState = 714;
		else fDone = true;
		break;
	    }
	}

	return iFound;
    }
}

class FirehoseBufferedInputStream extends InputStream {
    private final int m_cbDefault = 2048;
    private final int m_cbBack = 256;
    private byte[] m_rgbBuffer;
    private int m_cbBuffer;
    private int m_cbRequested;
    private int m_iBuffer;
    private InputStream m_is;

    public FirehoseBufferedInputStream(InputStream is)
	throws java.io.IOException {
	init(is, m_cbDefault);
    }

    public FirehoseBufferedInputStream(InputStream is, int cb)
	throws java.io.IOException {
	init(is, cb);
    }

    private void init(InputStream is, int cb) throws java.io.IOException {
	m_is = is;
	m_rgbBuffer = new byte[cb + m_cbBack];
	//Don't fill in the back buffer
	m_cbBuffer = m_is.read(m_rgbBuffer, m_cbBack, cb);
	if (m_cbBuffer != -1)
	    m_cbBuffer += m_cbBack;
	m_iBuffer = m_cbBack;
	m_cbRequested = cb;
    }

    public int available() throws java.io.IOException {
	return (m_cbBuffer - m_iBuffer) + m_is.available();
    }

    public int read() throws java.io.IOException {
	int b = read(new byte[1], 0, 1);
	return b;
    }

    public int read(byte[] rgbOut) throws java.io.IOException {
	return this.read(rgbOut, 0, rgbOut.length);
    }

    public int read(byte[] rgbOut, int iOffset, int iLength)
    throws java.io.IOException {
	int iOut = iOffset;

	while (iLength > 0 && m_cbBuffer != -1) {
	    int cbRead = Math.min(iLength, (m_cbBuffer - m_iBuffer));

	    //read what has been buffered
	    System.arraycopy(m_rgbBuffer, m_iBuffer, rgbOut, iOut, cbRead);
	    iOut += cbRead;
	    m_iBuffer += cbRead;
	    iLength -= cbRead;

	    //Did we hit the end?
	    if (m_iBuffer == m_cbBuffer) {
		//Save a bit of the end
		System.arraycopy(m_rgbBuffer, 0, m_rgbBuffer,
				 (m_cbBuffer - m_cbBack), m_cbBack);
		m_cbBuffer = m_is.read(m_rgbBuffer, m_cbBack, m_cbRequested);
		if (m_cbBuffer != -1)
		    m_cbBuffer += m_cbBack;
		m_iBuffer = m_cbBack;
	    }
	}

	if (m_cbBuffer == -1)
	    System.out.println("EOF occurred");
	return (m_cbBuffer == -1) ? -1 : (iOut - iOffset);
    }

    public long skip(long n) throws java.io.IOException {
	long cbSkipped = n;

	if (n < m_cbBuffer - m_iBuffer)
	    //If we have enough already buffered, just do it
	    m_iBuffer += n;
	else {
	    //Otherwise, only skip as many as is buffered, then
	    // reset our buffer.
	    if (m_cbBuffer == -1)
		cbSkipped = 0;
	    else {
		cbSkipped = m_cbBuffer - m_iBuffer;
		init(m_is, m_cbRequested);
	    }
	}

	if (cbSkipped == -1)
	    System.out.println("EOF occurred");
	return cbSkipped;
    }

    public void reset() throws java.io.IOException {
	m_is.reset();

	init(m_is, m_cbRequested);
    }

    public boolean markSupported() {
	//Because I'm chicken.
	return false;
    }
}

class ByteReader {
    byte [] m_rgb;
    int m_i;
    public ByteReader() {
	m_rgb = null;
	m_i = 0;
    }

    public ByteReader(byte[] rgb) {
	setByteArray(rgb);
    }

    public void setByteArray(byte[] rgb) {
	m_rgb = rgb;
	m_i = 0;
    }

    public int getOffset() {
	return m_i;
    }

    public int getLength() {
	return m_rgb.length;
    }

    public byte getByteAt(int i) {
	if (i >= m_rgb.length)
	    return 0;
	return m_rgb[i];
    }

    public String readLine() {
	return readLine(m_i);
    }

    public String readLine(int iOffset) {
	int c;
	String stOut = null;

	if (m_rgb == null || iOffset >= m_rgb.length)
	    return null;

	for (c = iOffset; c < m_rgb.length && m_rgb[c] != '\n'; c++);

	if (c > iOffset)
	    stOut = new String(m_rgb, iOffset, (c-iOffset-1));

	return stOut;
    }

    public boolean nextLine() {
	if (m_rgb == null || m_i >= m_rgb.length)
	    return false;

	while (m_i < m_rgb.length && m_rgb[m_i] != '\n')
	    m_i++;

	//Get it off the '\n'
	m_i++;
	return true;
    }

    public boolean startsWith(String st) {
	return startsWith(st, m_i);
    }

    public boolean startsWith(String st, int ib) {
	int ist;

	for (ist=0; ist < st.length(); ist++, ib++) {
	    if (m_rgb.length <= ib || st.charAt(ist) != m_rgb[ib])
		break;
	}

	return (ist == st.length());
    }

    public boolean lineContains(String st) {
	return lineContains(st, 0);
    }

    public boolean lineContains(String st, int ib) {
	ib += m_i;
	boolean fContains = false;
	for (; m_rgb.length > ib && m_rgb[ib] != '\n'; ib++) {
	    if (startsWith(st, ib)) {
		fContains = true;
		break;
	    }
	}

	return fContains;
    }
}

