package niagara.firehose;

import java.net.*;

// synchronized queue for communication between XMLListenerThread
// and XMLFirehoseThreads

class MsgQueue {
    private MsgQueueElem head;
    private MsgQueueElem tail;
    
    class MsgQueueElem {
	
	public MsgQueueElem next;
	public MsgQueueElem prev;
	public XMLGenMessage msg;
	
	public MsgQueueElem(XMLGenMessage _msg) {
	    next = prev = null;
	    msg = _msg;
	}
    }

    
    public MsgQueue() {
	head = tail = null;
    }

    public synchronized void put(XMLGenMessage _message) {
	// put message at head of queue
	
	// create new queue element
	MsgQueueElem new_elem = new MsgQueueElem(_message);
	
	if(head == null) {
	    // queue empty so must set tail pointer appropriately 
	    tail = new_elem;
	} else {
	    head.prev = new_elem;
	}
	new_elem.next = head;
	head = new_elem;
	
	// wake up someone
	notify();
    }

    public synchronized XMLGenMessage get() {
	try {
	    wait();
	} catch (InterruptedException e) {
	}
	
	// take item off of tail of queue and return
	XMLGenMessage ret_val = tail.msg;
	tail = tail.prev;
	if(tail == null) {
	    head = tail;
	} else {
	    tail.next = null;
	}
	return ret_val;
    }
}
