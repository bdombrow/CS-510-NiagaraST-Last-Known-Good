package niagara.firehose;

// encapsulates information about an open auction that must be saved

import java.util.*;

class OpenAuction {
    public static int MAXAUCTIONLEN_SEC = 24*60*60; // 24 hours
    public static int MINAUCTIONLEN_SEC = 2*60*60; // 2 hours

    private int currPrice; // price in dollars
    private boolean closed = false;
    private long endTime;
    int numBids = 0; // for debugging purposes
    private Random rnd;
    
    public OpenAuction(SimpleCalendar cal, int virtualId, Random rnd) {
	currPrice = rnd.nextInt(200)+1; // initial price must be at least $1
	endTime = cal.getTimeInSecs() + rnd.nextInt(MAXAUCTIONLEN_SEC) + MINAUCTIONLEN_SEC;
	this.rnd = rnd;
    }
    
    // increase the price, return the new bid amount
    public int increasePrice() {
	int increase = rnd.nextInt(25)+1; // zero increases not allowed
	currPrice += increase;
	return currPrice;
    }
    
    // curr price is always an even dollar amount
    public int getCurrPrice() {
	return currPrice;
    }
    
    public long getEndTime() {
	return endTime;
    }

    public boolean isClosed(SimpleCalendar cal) {
	checkClosed(cal);
	return closed;
    }

    public void recordBid() {
	numBids++;
    }

    private void checkClosed(SimpleCalendar cal) {
	if(!closed && (cal.getTimeInSecs())
	   > endTime) {
	    //System.out.println("KT closing auction. Number of Bids: " + numBids);
	    closed = true;
	}
	// KT - here is where we could create a closed_auction element
	// or do something to get a closed_auction created
	// but I'm not going to do it now
    }
}

