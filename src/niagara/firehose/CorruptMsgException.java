package niagara.firehose;

@SuppressWarnings("serial")
class CorruptMsgException extends Exception {
	public CorruptMsgException(String _str) {
		super(_str);
	}
}
