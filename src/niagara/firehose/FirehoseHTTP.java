package niagara.firehose;

// FirehoseHTTP - a simple web server which iterates through XML files
// by : Pornthep Nivatyakul
// FirehoseClient code added by Pete Tucker - 2/15/00

/* KT - I do not understand this code and don't have the time to
 * convert it to the new firehose client
import java.net.*;
import java.io.*;
import java.util.*;
import java.awt.*;
import java.awt.event.*;
import FirehoseClient.*;

class FirehoseHTTP extends Frame {
  static private TextArea m_ta;
  static private int m_portHTTP = 8088;

  class HTTPFrameListener implements WindowListener {
    public void windowActivated(WindowEvent e) {;}
    public void windowClosed(WindowEvent e) {;}
    public void windowClosing(WindowEvent e) {;}
    public void windowDeactivated(WindowEvent e) {;}
    public void windowDeiconified(WindowEvent e) {;}
    public void windowIconified(WindowEvent e) {;}
    public void windowOpened(WindowEvent e) {;}
  }

  private HTTPFrameListener m_fl = new HTTPFrameListener();

  public FirehoseHTTP() {
    setTitle("Firehose HTTP Server");
    Panel pnl = new Panel();
    pnl.setLayout(new FlowLayout());
    m_ta = new TextArea(8,70);
    add("Center", m_ta);
    addWindowListener(m_fl);
  }

  public void processEvent(AWTEvent evt) {
    if (evt.getID() == WindowEvent.WINDOW_CLOSING)
      System.exit(0);

    super.processEvent(evt);
  }

  public static void main(String[] args) {
    ServerSocket ss;
    int i, cThread = 0, portServer = FirehoseConstants.LISTEN_PORT;
    String stServer;
    try {
	stServer = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException ex) {
	stServer = new String("chinook.cse.ogi.edu");
    }
    Frame frm = new FirehoseHTTP();

    m_portHTTP = 8088;
    for (i = 0; i < args.length; i++) {
	//Has the user requested a specific port?
      if (args.length >= i + 1 &&
          (args[i].compareTo("-p") == 0 || args[i].compareTo("-P") == 0))
        m_portHTTP = Integer.parseInt(args[++i]);

      // Has the user requested a specific number of threads for the pool?
      if (args.length >= i + 1 &&
          (args[i].compareTo("-t") == 0 || args[i].compareTo("-T") == 0))
        cThread = Integer.parseInt(args[++i]);

      // On which port is the firehose server?
      if (args.length >= i + 1 &&
          (args[i].compareTo("-sp") == 0 || args[i].compareTo("-SP") == 0))
        portServer = Integer.parseInt(args[++i]);

      // On which machine is the firehose server?
      if (args.length >= i + 1 &&
          (args[i].compareTo("-sn") == 0 || args[i].compareTo("-SN") == 0))
        stServer = args[++i];
    }

    frm.setSize(400,300);
    frm.show();
    m_ta.append("Firehose HTTP Server Version 1.0   \n");
    m_ta.append("by Pete Tucker\n");
    m_ta.append("adapted from code by Pornthep Nivatyakul \n");
    m_ta.append(" \n");
    m_ta.append("waiting for connection  \n");
    m_ta.append("open your browser and type This Machine IP as URL: \n");
    m_ta.append("example : URL:  http://127.0.0.1:");
    m_ta.append(String.valueOf(m_portHTTP));
    m_ta.append("\n");
    
    JhttpPool jhp = new JhttpPool(cThread, portServer, stServer);

    try {
      ss = new ServerSocket(m_portHTTP);
      ss.getLocalPort();

      while (true) {
        Socket skt = ss.accept();
        jhp.newSocket(skt);
        Date now = new Date();
        m_ta.append("Accepting Connection - " + now + " \n");
      }
    } catch (IOException ex) {
      System.err.println("#1 error here!");
      System.err.println(ex.getMessage());
    }
  }
}

class FirehoseOutputStream  {
  OutputStream m_os;
  boolean m_fHideZero = true;
  String m_stDTDFile;

  public FirehoseOutputStream(OutputStream os) {
    m_os = os;
  }

  public void setDTDFile(String stDTDFile) {
    m_stDTDFile = stDTDFile;
  }

  public void hideZero(boolean fHide) { m_fHideZero = fHide; }

  public void write(String s) throws java.io.IOException {
      int iSystem = s.indexOf(" SYSTEM \"\"");
      StringBuffer stNew;

      // add the dtd into the data file - why do I need to do this??
      // and why here???
      if (iSystem != -1 && m_stDTDFile != null) {
	  stNew = new StringBuffer(s.substring(0, iSystem));
	  stNew.append(" SYSTEM \"" + m_stDTDFile + "\"");
	  stNew.append(s.substring(iSystem + (" SYSTEM \"\"").length()));
      }
    else
      stNew = new StringBuffer(s);

    byte[] rgs = stNew.toString().getBytes();
    int i, iLastNonZero = rgs.length;

    if (m_fHideZero) {
      iLastNonZero = 0;
      for (i = 0; i < rgs.length; i++) {
        if (rgs[i] == '\0')
          rgs[i] = (byte) ' ';
        else
          iLastNonZero = i + 1;
      }
    }

    m_os.write(rgs, 0, iLastNonZero);
  }

  public void close() throws java.io.IOException {
    m_os.close();
  }
}

class JhttpPool {
  private static int CTHREADDEFAULT = 25;
  private Vector m_vcSockets = new Vector();
  public RefreshThread m_rt = new RefreshThread();
  public int m_portServer;
  public String m_stServer;
  
  public JhttpPool(int cThread, int portServer, String stServer) {
    int i;
    JhttpThread jht;

    if (cThread == 0)
      cThread = CTHREADDEFAULT;

    m_portServer = portServer;
    m_stServer = stServer;

    for (i = 0; i < cThread; i++) {
      jht = new JhttpThread(this);
      jht.start();
    }

    m_rt.start();
  }
  
  public synchronized void newSocket(Socket skt) {
    m_vcSockets.add(skt);

    try {
      notify();
    } catch (Exception e) {
      System.out.println("1: Exception : " + e.toString());
    }
  }

  public synchronized Socket getSocket() {
    try {
      wait();
    } catch (Exception e) {
      System.out.println("2: Exception : " + e.toString());
    }

    return (Socket) m_vcSockets.remove(0);
  }
}

class ClientInfo {
    public InetAddress m_iaHost;
    public int m_iPort;
}

class RefreshThread extends Thread {
    private Vector m_vcClients = new Vector();

    public void addClient(ClientInfo c) {
	m_vcClients.add(c);
    }

    public void removeClient(ClientInfo c) {
	m_vcClients.remove(c);
    }

    public void run() {
	Vector vc;
	ClientInfo clt;
	Socket skt = null;
	OutputStream osClient = null;
	byte rgbNewData[] = {1,0,0,0};
	Random rnd = new Random();
	int iSec;

	while (true) {
	    try {
		//Sleep for a few seconds, then run through the
		// client list and let everyone know there is new
		// data.
		iSec = ((Math.abs(rnd.nextInt()) % 10) + 8) * 1000;
		Thread.sleep(iSec);
	    } catch (Exception e) {
		;
	    }

	    //Clone the vector, so there are no concurrency issues
	    vc = (Vector) m_vcClients.clone();

	    //iterate through the vector, notifying clients of new data
	    Iterator it = vc.iterator();
	    while (it.hasNext()) {
		clt = (ClientInfo) it.next();

		try {
		    skt = new Socket(clt.m_iaHost, clt.m_iPort);
		    osClient = skt.getOutputStream();
		    osClient.write(rgbNewData);
		} catch (Exception e) {
		    removeClient(clt);
		} finally {
		    try {
			osClient.close();
			skt.close();
		    } catch (Exception e){
			;
		    }
		}
	    }
	}
    }
}

class JhttpThread extends Thread {
    JhttpPool m_pool;
    static String m_STFILEROOT =
	new String("/disk/hopper/users/students/ptucker/niagara/Edgar/");
    static String m_STHTTPFILEROOT = new String("Edgar/");
    File m_fileRoot = new File(m_STFILEROOT);
    BufferedReader m_brdr;
    int m_iXMLBFile = 0;
  
    public JhttpThread(JhttpPool pool) {
	m_pool = pool;
    }

    public int abs(int x) { if (x >= 0) return x; else return (-x); }

    public void run() {
	String stMethod;
	String stVersion = "";
	int iPort;
	Socket sktConnection;
	FirehoseClient fc =
	    new FirehoseClient(); //m_pool.m_portServer, m_pool.m_stServer);

	while (true) {
	    sktConnection = m_pool.getSocket();
	    try {
		FirehoseOutputStream psOutput =
		    new FirehoseOutputStream(sktConnection.getOutputStream());
		BufferedReader brInput =
		    new BufferedReader(new InputStreamReader(sktConnection.getInputStream()));
		String stGet = brInput.readLine();
		if (stGet == null) {
		    sktConnection.close();
		    continue;
		}
		StringTokenizer stok = new StringTokenizer(stGet);
		
		stMethod = stok.nextToken();
		if (stMethod.equals("GET")) {
		    String stFile = stok.nextToken();
		    if ((iPort = stFile.indexOf("?REG")) != -1) {
			ClientInfo clt = new ClientInfo();
			clt.m_iaHost = sktConnection.getInetAddress();
			clt.m_iPort = Integer.parseInt(stFile.substring(iPort + 5));
			m_pool.m_rt.addClient(clt);
		    }

		    if (stok.hasMoreTokens())
			stVersion = stok.nextToken();

		    // loop through the rest of the input lines
		    while ((stGet = brInput.readLine()) != null) {
			if (stGet.trim().equals(""))
			    break;
		    }

		    try {
			if (stFile.startsWith("/DTD")) {
			    runFirehose(false, psOutput);
			} else if (stFile.startsWith("/XMLB")) {
			    runFirehose(psOutput);
			} else if (stFile.startsWith("/SUBFILE")) {
			} else if (stFile.startsWith("/TEMP")) {

			} else if (stFile.startsWith("/AUCTION")) {

			} else
			    runFile(stFile, psOutput);

			psOutput.close();
		    } catch (Exception e) {
			System.out.println("3: Exception - " + e.getMessage());

			// can't find the file
			if (stVersion.startsWith("HTTP/")) {
			    //  send a MIME header
			    psOutput.write("HTTP/1.0 404 File Not Found " + stFile + "\r\n");
			    Date now = new Date();
			    psOutput.write("Date: " + now + "\r\n");
			    psOutput.write("Server: FirehoseHTTP 1.0\r\n");
			    psOutput.write("Content-type text/html \r\n\r\n");
			}

			psOutput.write("<HTML><HEAD><TITLE>File Not Found</TITLE></HEAD>");
			psOutput.write("<BODY><H1>HTTP ERROR 404: ");
			psOutput.write("File not found in Directory.</H1>");
			psOutput.write(e.getMessage());
			psOutput.write("</BODY></HTML>");
			psOutput.close();
		    }
		} else {
		    // method does not equal "GET"
		    if  (stVersion.startsWith("HTTP/")) {
			psOutput.write("HTTP/1.0 501 Not Implement\r\n");
			Date now = new Date();
			psOutput.write("Date: " + now + "\r\n");
			psOutput.write("Server: FirehoseHTTP 1.0\r\n");
			psOutput.write("Content-type: text/html" + "\r\n\\r\n");
		    }

		    psOutput.write("<HTML><HEAD><TITLE>Not Implemented</TITLE></HEAD>");
		    psOutput.write("<BODY><H1>HTTP Error 501: Not  Implemented");
		    psOutput.write("</H1></BODY></HTML>");
		    psOutput.close();
		}
	    } catch (Exception e) {
		System.out.println("4: Exception : " + e.toString() + " - " + e.getMessage());
	    }
	    try {
		sktConnection.close();
	    } catch (Exception e) {
		System.out.println("5: Exception : " + e.toString() + " - " + e.getMessage());
	    }
	}
    }

    public void runFirehose(int streamDataType, FirehoseOutputStream psOutput) 
	throws java.io.IOException {
	FirehoseClient fc = new FirehoseClient();

	// was fXML == true, true on file, false on DTD
	ExtFilenameFilter eff = null;
	String stFileOut;
	if(streamDataType == DTD) {
	    new ExtFilenameFilter(".dtd");

	    // gets all files with extension eff from directory m_fileRoot
	    String[] rgstFiles = m_fileRoot.list((FilenameFilter) eff);
	    int iFile = abs((int) System.currentTimeMillis()) % rgstFiles.length;
	    stFileOut = new String(m_STFILEROOT + rgstFiles[iFile]);
	}

	psOutput.setDTDFile(m_STHTTPFILEROOT + rgstFiles[iFile]);

	FirehoseSpec fhSpec = new FirehoseSpec(FirehoseConstants.LISTEN_PORT,
			             InetAddress.getLocalHost().getHostName(),
					       streamDataType,
					       stFileOut,
					       1,
					       1,
					       1,
					       false);
	InputStream fIs = fc.open_stream(fhSpec);
	if (fc.open_stream(1, stype, stFileOut, 1, 1) == -1)
	    System.out.println("An error occurred opening the stream...");

	psOutput.write("HTTP/1.0 200 OK\r\n");
	Date now = new Date();
	psOutput.write("Date: " + now + "\r\n");
	psOutput.write("Server: FirehoseHTTP 1.0\r\n");
	psOutput.write("Content-type text/html" + " \r\n\r\n");

	// send the file
	String stOut;
	int err = 0;
	boolean eof = false;
	while (err == 0 && eof == false) {
	    err = fc.get_data();
	    if (err == 0) {
		stOut = fc.get_buffer();
		psOutput.write(stOut);
		eof = fc.get_eof();
	    }
	}
    }

    public void runXMLB(FirehoseOutputStream psOutput) 
    throws java.io.IOException {
	int iElement = -1;
	String stEnd = new String();
	StringBuffer stb = new StringBuffer();

	if (m_brdr == null) {
	    setupReader();
	    /*
	      //FirehoseClient fc = new FirehoseClient();

	//int stype = FirehoseClient.FCSTREAMTYPE_FILE;

	// only need numTLElts for XMLB
	//FirehoseSpec fhSpec = new FirehoseSpec(listenerHostName, // dont need
	 //                                      listenerPortNum, // dont need
          //                                     id,
           //                                    dataType,
           //                                    descriptor,
            //                                   numGenCalls,
           //                                    numTLElts,
            //                                   rate);

	//if (fc.open_stream(1, stype, stFileOut, 1, 1) == -1)
	 //   System.out.println("An error occurred opening the stream...");

	// send the file
	//StringBuffer stbOut = new StringBuffer();
	//int err = 0;
	//boolean eof = false;
	//while (err == 0 && eof == false) {
	 //   err = fc.get_data();
	  //  if (err == 0) {
	//	stbOut.append(fc.get_buffer());
	//	eof = fc.get_eof();
	 //   }
	//}

	//m_brdr = new BufferedReader(new StringReader(stbOut.toString()));
   // }
	}

	psOutput.write("HTTP/1.0 200 OK\r\n");
	Date now = new Date();
	psOutput.write("Date: " + now + "\r\n");
	psOutput.write("Server: FirehoseHTTP 1.0\r\n");
	psOutput.write("Content-type text/html" + " \r\n\r\n");

	String stLine = new String();
	while (iElement == -1) {
	    stLine = m_brdr.readLine();
	    if (stLine == null)
		setupReader();
	    else {
		if (stLine.indexOf("<PLAY>") != -1) {
		    stEnd = new String("</PLAY>");
		    stb.append("<?xml version = \"1.0\"?>\n");
		    stb.append("<!DOCTYPE PLAY SYSTEM \"XMLB/gamedata/gamedata.dtd\">\n");
		    iElement = 0;
		} else if (stLine.indexOf("<GAME_START>") != -1) {
		    stEnd = new String("</GAME_START>");
		    stb.append("<?xml version = \"1.0\"?>\n");
		    stb.append("<!DOCTYPE GAME_START SYSTEM \"XMLB/gamedata/gamedata.dtd\">\n");
		    iElement = 1;
		} else if (stLine.indexOf("<GAME_END>") != -1) {
		    stEnd = new String("</GAME_END>");
		    stb.append("<?xml version = \"1.0\"?>\n");
		    stb.append("<!DOCTYPE GAME_END SYSTEM \"XMLB/gamedata/gamedata.dtd\">\n");
		    iElement = 2;
		}
	    }
	}
	
	while (stLine.indexOf(stEnd) == -1) {
	    stb.append(stLine);
	    stb.append("\n");
	    stLine = m_brdr.readLine();
	}
	//Get the end token
	stb.append(stLine);
	stb.append("\n");

	psOutput.write(stb.toString());
    }

    public void runFile(String stFileIn, FirehoseOutputStream psOutput)
	throws java.io.IOException {
	//The incoming string has a leading '/', so cut it off
	FileInputStream fis = new FileInputStream(stFileIn.substring(1));
	String stOut;
	byte[] rgbIn = new byte[0x1000];
	int cbIn;

	psOutput.write("HTTP/1.0 200 OK\r\n");
	Date now = new Date();
	psOutput.write("Date: " + now + "\r\n");
	psOutput.write("Server: FirehoseHTTP 1.0\r\n");
	if (stFileIn.endsWith(".class") == false) {
	    psOutput.write("Content-type text/html" + " \r\n\r\n");
	}
	else {
	    psOutput.hideZero(false);
	    psOutput.write("Content-type application/octet-type" + " \r\n\r\n");
	}

	cbIn = fis.read(rgbIn);
	while (cbIn != -1) {
	    stOut = new String(rgbIn, 0, cbIn);
	    psOutput.write(stOut);
	    cbIn = fis.read(rgbIn);
	}
    }
    
    private void setupReader() {
    FirehoseSpec fhSpec = new FirehoseSpec(...)
	FirehoseClient fc = new FirehoseClient(fhSpec);


	// send the file
	StringBuffer stbOut = new StringBuffer();
	int err = 0;
	boolean eof = false;
	while (err == 0 && eof == false) {
	    err = fc.get_data();
	    if (err == 0) {
		stbOut.append(fc.get_buffer());
		eof = fc.get_eof();
	    }
	}

	m_brdr = new BufferedReader(new StringReader(stbOut.toString()));
    }
}


*/
