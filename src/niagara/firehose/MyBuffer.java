package niagara.firehose;

import java.io.*;
import java.nio.*;

// Think char buf was the fastest..., or faster than using the writer

class MyBuffer {
    CharBuffer cb = CharBuffer.allocate(8192);

    public void append(String s) {
	cb.put(s);
    }
    public void append(int i){
	if(i<IntToString.NUM_VALS)
	    cb.put(IntToString.strings[i]);
	else
	    cb.put(String.valueOf(i));	    
    }
    public void append(long l) {
	if(l<IntToString.NUM_VALS) {
	    cb.put(IntToString.strings[(int)l]);
	} else {
	    cb.put(String.valueOf(l));	    
	}
    }
    /*    public void append(double d) {
	cb.put(String.valueOf(d));
	}*/
    public void append(CharBuffer other) {
	cb.put(other.array(), 0, other.position());
    }
    public void clear(){
	// stringBuf.setLength(0);
	cb.clear();
    }
    public char[] array() {
	return cb.array();
    }
    // number of characters that have been put into this buffer
    public int length() {
	return cb.position();
    }
}
