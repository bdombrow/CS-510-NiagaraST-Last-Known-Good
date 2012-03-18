package niagara.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@SuppressWarnings("serial")
public class DisplayServlet extends HttpServlet {

	// private static boolean doDebug = false;
	public void doPost(HttpServletRequest req, HttpServletResponse res)
			throws IOException {
		BufferedReader in = new BufferedReader(new InputStreamReader(req
				.getInputStream()));
		if (!MQPClient.quiet)
			System.out.println("<niagararesults query_id='" + in.readLine()
					+ "'>");
		String input = null;
		while ((input = in.readLine()) != null) {
			if (!MQPClient.quiet)
				System.out.println(input);
		}
		if (!MQPClient.quiet)
			System.out.println("</niagararesults>");
		res.getOutputStream().close();
		MQPClient.getMQPClient().queryDone();
	}
}
