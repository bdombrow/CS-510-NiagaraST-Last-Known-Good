/* main program for the java firehose generator program */

package niagara.firehose;

class XMLFirehose {
    public static void main(String[] args) {
	// get port number
	int i=0;
	Integer port_num = new Integer(FirehoseConstants.LISTEN_PORT);
	boolean ok = true;

	while(i < args.length && ok) {
	    ok = false;

	    //Let the user set the port to listen on
	    if(i<args.length && args[i].equals("-p")) {
		try {
		    port_num = Integer.decode(args[i+1]);
		} catch (NumberFormatException e) {
		    System.out.println("Conversion error " + e.getMessage());
		    return;
		}
		i+=2;
		ok = true;
	    }
	}

	if(!ok) {
	    System.out.println("Usage: FirehoseGenerator " + 
			       "[-p port_num]");
	    return;
	}

	// now start the listener
	XMLListenerThread listener = new XMLListenerThread("Listener");
	listener.run(port_num.intValue());
	return;
    }
}
