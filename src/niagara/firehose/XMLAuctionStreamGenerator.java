package niagara.firehose;

import java.io.*;
import java.net.*;
import java.util.*;
import java.lang.reflect.Array;

class XMLAuctionStreamGenerator extends XMLFirehoseGen {
    String tab = "";
    String tab2 = "";
    String tab3 = "";
    String nl = "";

    public static int ITEMS_PER_PERSON = 10;
    public static int BIDS_PER_ITEM = 10;
    public static final String yesno[] = { "yes", "no"};
    public static final String auction_type[] = {"Regular", "Featured"};
    
    // will generate bids, items and persons in a ratio of 10 bids/item 5 items/person
    private Random  rnd = new Random(103984);
    private SimpleCalendar cal = new SimpleCalendar(rnd);
    
    private long idTrace = 0;
    private Writer wrtTrace;

    private Persons persons = new Persons(); // for managing person ids
    private OpenAuctions openAuctions; // for managing open auctions
    private PersonGen p = new PersonGen();  // used for generating values for person    

    // TODO - change to writing into a byte array... KT
    private MyBuffer myBuf;
    private XMLFirehoseThread writer;
    private int numGenCalls;
    
    public XMLAuctionStreamGenerator(int numTLElts,
				     int numGenCalls, boolean streaming,
				     boolean prettyPrint, BufferedWriter wrtTrace) {
	// numTLElts is ignored for now, but leave it in case I change my mind - KT
	this.numTLElts = numTLElts; 
	this.numGenCalls = numGenCalls;
	usePrettyPrint = prettyPrint;
	useStreamingFormat = streaming;
	this.wrtTrace = wrtTrace;
	openAuctions = new OpenAuctions(cal);

	if(usePrettyPrint) {
	    tab = "\t";
	    tab2 = "\t\t";
	    tab3 = "\t\t\t";
	    nl = "\n";
	}
    }

    public void generateStream(XMLFirehoseThread writer) throws IOException {
	myBuf = new MyBuffer();
	this.writer = writer;
	
	// first do startup - generate some people and open auctions that
	// can be bid on
	
	for(int i = 0;i<50; i++) {
	    initMyBuf();
	    generatePerson(myBuf, 1);
	    writeMyBuf();
	}
	for(int i = 0; i<50; i++) {
	    initMyBuf();
	    generateOpenAuction(myBuf, 1);
	    writeMyBuf();
	}

	// now go into a loop generating bids and persons and so on
	// want on average 10 items/person and 10 bids/item
	int count = 0;
	while(count < numGenCalls) {
	    initMyBuf();

	    // generating a person approximately 10th time will
	    // give is 10 items/person since we generate on average
	    // one bid per loop
	    if(rnd.nextInt(10) == 0) {
		generatePerson(myBuf, 1);
	    } 
	    
	    // want on average 1 item and 10 bids
	    int numItems = rnd.nextInt(3); // should average 1
	    generateOpenAuction(myBuf, numItems);
	    
	    int numBids = rnd.nextInt(20); // should average 10
	    generateBid(myBuf, numBids);

	    writeMyBuf();
	    count++;
	}
	
	if (wrtTrace != null) {
	    wrtTrace.close();
	}
    }

    private void initMyBuf() throws IOException {
	myBuf.clear();
	if(!useStreamingFormat)
	   myBuf.append(FirehoseConstants.XML_DECL);
	myBuf.append("<site>");
	myBuf.append(nl);
    }
    
    private void writeMyBuf() throws IOException {
	myBuf.append("</site>");
	myBuf.append(nl);
	writer.write_chars(myBuf.array(), myBuf.length());
    } 
    
    private void generateBid(MyBuffer myb, int numBids) throws IOException {
	cal.incrementTime();
	myb.append("<open_auctions");
	if (wrtTrace != null)
	    writeTrace(myb);
	myb.append(">");
	
	myb.append(nl);
	
	for (int i=0; i<numBids;i++) {
	    int itemId = openAuctions.getExistingId(); 
	    myb.append(tab);
	    myb.append("<open_auction id=\"");
	    myb.append(itemId);
	    myb.append("\">");
	    myb.append(nl);
	 
	    myb.append(tab2);
	    myb.append("<bidder>");
	    myb.append(nl);
	    
	    myb.append(tab3);
	    myb.append("<time>");
	    myb.append(cal.getTimeInSecs());
	    myb.append("</time>");
	    myb.append(nl);
	    
	    myb.append(tab3);
	    myb.append("<person_ref person=\"");
	    myb.append(persons.getExistingId()); 
	    myb.append("\"></person_ref>");
	    myb.append(nl);
	    
	    myb.append(tab3);
	    myb.append("<bid>");
	    myb.append(openAuctions.increasePrice(itemId));
	    myb.append(".00</bid>");
	    myb.append(nl);
	    
	    myb.append(tab2);
	    myb.append("</bidder>");
	    myb.append(nl);
	    
	    myb.append(tab);
	    myb.append("</open_auction>");
	    myb.append(nl);
	}
	myb.append("</open_auctions>");
	myb.append(nl);	
    }

    public boolean generatesStream() { return true;}
    
    // uugh, a bad thing here is that a person can be selling items that are in
    // different regions, ugly, but to keep it consistent requires maintaining
    // too much data and also I don't think this will affect results
    private void generateOpenAuction(MyBuffer myb, int numItems) throws IOException {
	cal.incrementTime();

	myb.append("<open_auctions");
	if (wrtTrace != null)
	    writeTrace(myb);
	myb.append(">");
	
	myb.append(nl);
	
	// open auction contains:
	// initial, reserve?, bidder*, current, privacy?, itemref, seller, annotation, 
	// quantity, type, interval    
	
	for (int i=0; i<numItems; i++) {
	    // at this point we are not generating items, we are generating
	    // only open auctions, id for open_auction is same as id of item
	    // up for auction
	    
	    myb.append(tab);
	    myb.append("<open_auction id=\"");
	    int auctionId = openAuctions.getNewId();
	    myb.append(auctionId);
	    myb.append("\">");
	    myb.append(nl);

	    // no initial - does not fit our scenario
	    
	    // reserve 
	    if(rnd.nextBoolean()) {
		myb.append(tab2);
		myb.append("<reserve>");
		// was generated using an exponential distribution, but I changed
		// it to uniform KT
		// HERE - TODO
		myb.append((int)Math.round((openAuctions.getCurrPrice(auctionId))*(1.2+(rnd.nextDouble()+1))));
		myb.append("</reserve>");
		myb.append(nl);
	    }
	    
	    // no bidders
	    
	    // no current - do with accumlator
	    
	    // privacy 
	    if(rnd.nextBoolean()) {
		myb.append(tab2);
		myb.append("<privacy>");
		myb.append(yesno[rnd.nextInt(2)]);
		myb.append("</privacy>");
		myb.append(nl);
	    }
	    
	    // itemref
	    myb.append(tab2);
	    myb.append("<itemref item=\"");
	    // assume itemId and openAuctionId are same - only one auction per item allowed
	    myb.append(auctionId);
	    myb.append("\"></itemref>");
	    myb.append(nl);
	    
	    // seller
	    myb.append(tab2);
	    myb.append("<seller person=\"");
	    myb.append(persons.getExistingId());
	    myb.append("\"></seller>");
	    myb.append(nl);
	    
	    // skip annotation - too hard to generate - need to just get this done KT

	    // KT - add category id XMark items can be in 1-10 categories
	    // we allow an item to be in one category
	    myb.append(tab2);
	    myb.append("<category>");
	    int catid = rnd.nextInt(303);
	    myb.append(catid);
	    myb.append("</category>");
	    myb.append(nl);

	    // quantity
	    myb.append(tab2);
	    myb.append("<quantity>");
	    int quantity = 1+rnd.nextInt(10);
	    myb.append(quantity);
	    myb.append("</quantity>");
	    myb.append(nl);
	    
	    // type
	    myb.append(tab2);
	    myb.append("<type>");
	    myb.append(auction_type[rnd.nextInt(2)]);
	    if(quantity>1 && rnd.nextBoolean())
		myb.append(", Dutch"); // 
	    myb.append("</type>");
	    myb.append(nl);
	    
	    // interval
	    myb.append(tab2);
	    myb.append("<interval>");
	    myb.append("<start>");
	    myb.append(cal.getTimeInSecs());
	    myb.append("</start>");
	    myb.append("<end>");
	    myb.append(openAuctions.getEndTime(auctionId));
	    myb.append("</end>");
	    myb.append("</interval>");
	    myb.append(nl);
	
	    myb.append(tab);
	    myb.append("</open_auction>");
	    myb.append(nl);
	}
	myb.append("</open_auctions>");
	myb.append(nl);
    }
    
    // append region AFRICA, ASIA, AUSTRALIA, EUROPE, NAMERICA, SAMERICA
    //Item contains:
    // location, quantity, name, payment, description, shipping, incategory+, mailbox)>
    // weird, item doesn't contain a reference to the seller, open_auction contains
    // a reference to the item and a reference to the seller
    
  
    public void generatePerson(MyBuffer myb, int numPersons) throws IOException {
	cal.incrementTime();

	myb.append("<people");
	
	if (wrtTrace != null)
	    writeTrace(myb);
	myb.append(">");      
	myb.append(nl);

	for (int i=0; i<numPersons; i++) {
	    p.generateValues(openAuctions); // person object is reusable now
	    
	    myb.append(tab);
	    myb.append("<person id=\"");
	    myb.append(persons.getNewId()); 
	    myb.append("\">");
	    myb.append(nl);
	    
	    myb.append(tab2);
	    myb.append("<name>");
	    myb.append(p.m_stName);
	    myb.append("</name>");
	    myb.append(nl);

	    myb.append(tab2);
	    myb.append("<emailaddress>");
	    myb.append(p.m_stEmail);
	    myb.append("</emailaddress>");
	    myb.append(nl);

	    if (p.has_phone) {
		myb.append(tab2);
		myb.append("<phone>");
		myb.append(p.m_stPhone);
		myb.append("</phone>");
		myb.append(nl);
	    }
	    if (p.has_address) {
		myb.append(tab2);
		myb.append("<address>");
		myb.append(nl);
		
		myb.append(tab3);
		myb.append("<street>");
		myb.append(p.m_address.m_stStreet);
		myb.append("</street>");
		myb.append(nl);
		
		myb.append(tab3);
		myb.append("<city>");
		myb.append(p.m_address.m_stCity);
		myb.append("</city>");
		myb.append(nl);
		
		myb.append(tab3);
		myb.append("<country>");
		myb.append(p.m_address.m_stCountry);
		myb.append("</country>");
		myb.append(nl);
		
		myb.append(tab3);
		myb.append("<province>");
		myb.append(p.m_address.m_stProvince);
		myb.append("</province>");
		myb.append(nl);
		
		myb.append(tab3);
		myb.append("<zipcode>");
		myb.append(p.m_address.m_stZipcode);
		myb.append("</zipcode>");
		myb.append(nl);
		
		myb.append(tab2);
		myb.append("</address>");
		myb.append(nl);
	    }
	    if (p.has_homepage) {
		myb.append(tab2);
		myb.append("<homepage>");
		myb.append(p.m_stHomepage);
		myb.append("</homepage>");
		myb.append(nl);
	    }
	    if (p.has_creditcard) {
		myb.append(tab2);
		myb.append("<creditcard>");
		myb.append(p.m_stCreditcard);
		myb.append("</creditcard>");
		myb.append(nl);
	    }
	    
	    if (p.has_profile) {
		myb.append(tab2);
		myb.append("<profile income=\"");
		myb.append(p.m_profile.m_stIncome);
		myb.append("\">");
		myb.append(nl);

		for (int j=0; j < p.m_profile.m_vctInterest.size(); j++) {
		    myb.append(tab3);
		    myb.append("<interest category=\"");
		    myb.append((String)p.m_profile.m_vctInterest.get(j));
		    myb.append("\"/>");
		    myb.append(nl);
		}
		if (p.m_profile.has_education) {
		    myb.append(tab3);
		    myb.append("<education>");
		    myb.append(p.m_profile.m_stEducation);
		    myb.append("</education>");
		    myb.append(nl);
		}
		if (p.m_profile.has_gender) {
		    myb.append(tab3);
		    myb.append("<gender>");
		    myb.append(p.m_profile.m_stGender);
		    myb.append("</gender>");
		    myb.append(nl);
		}
		
		myb.append(tab3);
		myb.append("<business>");
		myb.append(p.m_profile.m_stBusiness);
		myb.append("</business>");
		myb.append(nl);
		
		if (p.m_profile.has_age) {
		    myb.append(tab3);
		    myb.append("<age>");
		    myb.append(p.m_profile.m_stAge);
		    myb.append("</age>");
		    myb.append(nl);
		}
		myb.append(tab2);
		myb.append("</profile>");
		myb.append(nl);
	    }
	    if (p.has_watches) {
		myb.append(tab2);
		myb.append("<watches>");
		myb.append(nl);
		for (int j=0; j<p.m_vctWatches.size(); j++) {
		    myb.append(tab3);
		    myb.append("<watch>");
		    myb.append((String)p.m_vctWatches.get(j));
		    myb.append("</watch>");
		    myb.append(nl);
		}
		myb.append(tab2);
		myb.append("</watches>");
		myb.append(nl);
	    }
	    
	    myb.append(tab);
	    myb.append("</person>");
	    myb.append(nl);
	}
	myb.append("</people>");
	myb.append(nl);
    }
    
    private void writeTrace(MyBuffer myb) throws IOException {
	long ts = System.currentTimeMillis();
	
	//The trace contains a unique ID, and a timestamp, so that
	// we can determine latency at the client
	wrtTrace.write(String.valueOf(idTrace));
	wrtTrace.write(",");
	wrtTrace.write(String.valueOf(ts));
	wrtTrace.write("\n");

	myb.append(" id=\"");
	myb.append(idTrace++);
	myb.append("\" ts=\"");
	myb.append(ts);
	myb.append("\"");
    }
}

