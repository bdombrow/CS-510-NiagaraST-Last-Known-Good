package niagara.firehose;

import java.applet.*;
import java.net.*;
import java.io.InputStream;

class RefreshXML extends Thread {
  Applet m_aptContainer;

  public RefreshXML(Applet aptContainer) {
    m_aptContainer = aptContainer;
  }

  public void run() {
    int i = 0;

    try {
      ServerSocket ss = new ServerSocket(0);  //Get me any available port.
      Socket skt;
      InputStream is;
      byte rgbMsg[] = new byte[4];
      String stURL = new String("http://church.cse.ogi.edu:8088/FILE?REG=" + 
				String.valueOf(ss.getLocalPort()));

      m_aptContainer.getAppletContext().showDocument(new URL(stURL), "xml");

      while (true) {
        skt = ss.accept();
        is = skt.getInputStream();

	m_aptContainer.getAppletContext().showStatus("Waiting for input...");
	is.read(rgbMsg, 0, 4);

	if (rgbMsg[0] == 1 && rgbMsg[1] == 0 && rgbMsg[2] == 0 && rgbMsg[3] == 0) {
	    m_aptContainer.getAppletContext().showStatus("Reloading...");
	    // update the xml frame with new data
	    m_aptContainer.getAppletContext().showDocument(new URL("http://church.cse.ogi.edu:8088/FILE" + i), "xml");

	    i = (i + 1) % 1000;
	}

	is.close();
	skt.close();
      }
    } catch (Exception e) {
      m_aptContainer.getAppletContext().showStatus("Exception - " + e.toString());
    }
  }
}

class FirehoseReloadXML extends Applet {
  public void start() {
      RefreshXML rx = new RefreshXML(this);

      rx.start();
  }
}
