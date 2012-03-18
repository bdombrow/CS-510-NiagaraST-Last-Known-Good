package niagara.client;

import gnu.regexp.RE;
import gnu.regexp.REMatch;

import java.io.BufferedReader;

import niagara.utils.PEException;

class SimpleConnectionReader extends AbstractConnectionReader implements
		Runnable {
	UIDriverIF ui;

	public SimpleConnectionReader(String hostname, int port, UIDriverIF ui) {
		super(hostname, port, ui);
		this.ui = ui;
	}

	StringBuffer results = new StringBuffer();

	synchronized private void addResult(String line) {
		results.append(line);
	}

	synchronized public String getResults() {
		String resultStr = results.toString();
		results.setLength(0);
		return resultStr;
	}

	synchronized public void appendResults(StringBuffer sb) {
		sb.append(results);
		results.setLength(0);
	}

	/**
	 * The run method accumulates result strings
	 */
	public void run() {
		int local_id = -1, server_id = -1;
		// Read the connection and throw the callbacks

		boolean end = false;
		BufferedReader br = new BufferedReader(cReader);
		try {
			((SimpleClient) ui).setConnectionReader(this);

			String line;
			RE re = new RE(
					"<responseMessage .*localID\\s*=\\s*\"([0-9]*)\"\\s*serverID\\s*=\\s*\"([0-9]*)\"\\s*responseType\\s*=\\s*\"server_query_id\"");
			RE reLid = new RE("<responseMessage .*localID\\s*=\\s*\"([0-9]*)\"");
			RE reLidNoPad = new RE(
					"SERVER ERROR - localID\\s*=\\s*\"([0-9]*)\"");
			boolean registered = false;

			line = br.readLine();
			while (line != null) {
				if (line.indexOf("<response") == 0) {
					if (!registered
							&& line.indexOf("\"server_query_id\"") != -1) {
						REMatch m = re.getMatch(line);
						local_id = Integer.parseInt(m.substituteInto("$1"));
						server_id = Integer.parseInt(m.substituteInto("$2"));
						queryRegistry.setServerId(local_id, server_id);
					}
					if (line.indexOf("error") != -1) {
						REMatch m = reLid.getMatch(line);
						local_id = Integer.parseInt(m.substituteInto("$1"));
						line = br.readLine(); // response data line
						line = br.readLine(); // should be the error message
						ui.errorMessage(local_id, line + "\n");
					}
					if (line.indexOf("\"end_result\">") != -1) {
						addResult("\n</niagararesults>");
						ui.notifyNew(local_id);
						ui.notifyFinalResult(local_id);
						end = true;
					}
				} else if (line.indexOf("</response") == 0) {
					// ignore line
				} else if (line.indexOf("ServerError") != -1) {
					// handle the non-padded stuff
					REMatch m = reLidNoPad.getMatch(line);
					local_id = Integer.parseInt(m.substituteInto("$1"));
					ui.errorMessage(local_id, line + "\n");
				} else {
					// KT - HERE IS WHERE CLIENT RESULTS ARE PRODUCED
					addResult(line);
					if (line.indexOf("<?xml") != -1)
						addResult("\n<niagararesults>\n");
					ui.notifyNew(local_id);
				}
				line = br.readLine();
			}
			br.close();
		} catch (gnu.regexp.REException e) {
			e.printStackTrace();
			throw new PEException("Invalid response message reg exception "
					+ e.getMessage());

		} catch (java.io.IOException ioe) {
			if (!end) {
				System.err.println("Unable to read from server "
						+ ioe.getMessage());
				ioe.printStackTrace();
				throw new PEException("Unable to read from server");
			}
		}
	}
}
