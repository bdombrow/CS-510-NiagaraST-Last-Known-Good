package niagara.firehose;

import java.lang.*;


class CorruptMsgException extends Exception {
    public CorruptMsgException(String _str) {
	super(_str);
    } 
}
