package niagara.utils;

/***
 * 
 * @author rfernand
 *
 * ControlMessage objects carry a Control Flag and a Message string. 
 * They are used for various Windowing functions and to drive PunctQC/DBScan interaction.
 */
public class ControlMessage {

	// Fields
	private ControlFlag _flag;
	private String _message;

	// Properties
	/***
	 * @return Control Flag
	 */
	public ControlFlag flag() {
		return _flag;
	}

	/***
	 * @return Control Message
	 */
	public String message() {
		return _message;
	}

	// Ctor
	/***
	 * @param flag Control Flag
	 * @param message Control message
	 */
	public ControlMessage(ControlFlag flag, String message) {
		this._flag = flag;
		this._message = message;
	}
}
