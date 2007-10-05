package niagara.utils;

/** A mechanism for notifying a single consumer thread when one or more 
 *  producer threads has produced something. 
 *
 * XXX vpapad: There must be a proper CS name for this structure, 
 * but I can't be bothered to look it up. */


public class MailboxFlag {
    private boolean flag;

    public synchronized void raise() {
	if (flag)
	    return;

	flag = true;
	notify();
    }

    public synchronized void waitOn() {
	while (!flag) {
	    try {
		wait();
	    } catch (InterruptedException e) {
		// Do nothing
	    }
	}

	flag = false;
    }
}

