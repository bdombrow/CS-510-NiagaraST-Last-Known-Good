package niagara.firehose;

import java.util.*;
import niagara.utils.*;

class SimpleCalendar {
    private static int MAXINCREMENT_SEC = 60;
    private int time_sec = 0; // time in seconds
    private Random rnd;

    SimpleCalendar(Random rnd) {	
	this.rnd = rnd;
    }

    int getTimeInSecs() {
	return time_sec;
    }
    
    void incrementTime() {
	time_sec += rnd.nextInt(MAXINCREMENT_SEC); // 1000 millesecons per second
	if(time_sec < 0)
	    throw new PEException("time overflowed");
    }
}
