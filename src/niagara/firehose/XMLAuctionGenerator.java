package niagara.firehose;

import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Random;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.AttributeList;
import org.xml.sax.HandlerBase;
import org.xml.sax.SAXException;

class XMLAuctionGenerator extends XMLFirehoseGen {
    private static AuctionValues m_av = null;
    private String stFile;
    private boolean doBid;
    private boolean doPerson;
    String tab = "";
    String tab2 = "";
    String tab3 = "";
    String nl = "";
    private long idTrace = 0;
    private Writer wrtTrace;
    StringBuffer stringBuf;
    GregorianCalendar cal;
    SimpleDateFormat sdf;

  public XMLAuctionGenerator(String stFile, String desc2,
			     int numTLElts, boolean streaming,
			     boolean prettyPrint, Writer wrtTrace) {
      this.stFile = stFile;
      doBid = (desc2.indexOf("bid") != -1);
      doPerson = (desc2.indexOf("person") != -1);

      this.numTLElts = numTLElts;
      this.wrtTrace = wrtTrace;
      if (m_av == null)
	  m_av = new AuctionValues();
      useStreamingFormat = streaming;
      usePrettyPrint = prettyPrint;
      stringBuf = new StringBuffer();

      if(usePrettyPrint) {
	  tab = "   ";
	  tab2 = tab + tab;
	  tab3 = tab + tab + tab;
	  nl = "\n";
      }

      cal = new GregorianCalendar();
      cal.set(2001,10,2,1,12,53);

      //stFile points to the existing auction file
      //Get the auction values
      m_av.init(stFile, cal);

      sdf = new SimpleDateFormat("'<date>'MM/dd/yyyy'</date>" +
				 "<time>'hh:mm:ss'</time>'");
  }
  
  public String generateXMLString() {
      // KT - appears we can ignore useStreamingFormat - don't see
      // the xml header being added anywhere

    stringBuf.setLength(0);
    
    //System.out.println("Initial Auction Values: " +
    //		       m_av.getPersonCount() + " persons, " + 
    //		       m_av.getItemCount() + " items, " + 
    //		       m_av.getOpenAuctionCount() + " open auctions");

    // Generate appropriate data
    stringBuf.append("<site>");
    stringBuf.append(nl);
    if(doBid) {
	generateBid(stringBuf, numTLElts);
    }
    if(doPerson) {
	generatePerson(stringBuf, numTLElts);
    }
    stringBuf.append("</site>");
    stringBuf.append(nl);

    return stringBuf.toString();
  }

  private void generateBid(StringBuffer stb, int numBids) {
    Random rnd = new Random();
    int iPerson, iItem, iAuction;
    
    iPerson = rnd.nextInt(m_av.getPersonCount());
    iItem = rnd.nextInt(m_av.getItemCount());
    iAuction = rnd.nextInt(m_av.getOpenAuctionCount());

    // KT - if this is too slow - make two cases with pretty print and
    // without to reduce calls to append
    stb.append("<open_auctions");
    if (wrtTrace != null)
	writeTrace(stb);
    stb.append(">");
    stb.append(nl);
    for (int iDoc=0; iDoc<numBids;iDoc++) {
	stb.append(tab);
	stb.append("<open_auction id=\"");
	stb.append(m_av.getOpenAuction(iAuction));
	stb.append("\">");
	stb.append(nl);
	
	stb.append(tab2);
	stb.append("<bidder>");
	stb.append(sdf.format(m_av.getCurrentTime()));
	stb.append(nl);
	
	stb.append(tab3);
	stb.append("<person_ref person=\"");
	stb.append(m_av.getPerson(iPerson));
	stb.append("\"/>");
	stb.append(nl);
	
	stb.append(tab3);
	stb.append("<increase>");
	stb.append(rnd.nextInt(12));
	stb.append(".00</increase>");
	stb.append(nl);
	
	stb.append(tab3);
	stb.append("<itemref item=\"");
	stb.append(m_av.getItem(iItem));
	stb.append("\"/>");
	stb.append(nl);
	
	stb.append(tab2);
	stb.append("</bidder>");
	stb.append(nl);
	
	stb.append(tab);
	stb.append("</open_auction>");
	stb.append(nl);
    }
    stb.append("</open_auctions>");
    stb.append(nl);
  }
  
  public void generatePerson(StringBuffer stb, int numPersons) {

    // KT - if we need more speed - remove the pluses and also - to avoid excessive
    // calls to append - remove the +s.
    
    stb.append("<people");
    if (wrtTrace != null)
	writeTrace(stb);
    stb.append(">");
    stb.append(nl);
    StringBuffer strbuf = new StringBuffer("person");
    for (int iDoc=0; iDoc<numPersons;iDoc++) {
      Person p = new Person();
      p.init(m_av);
      String id = m_av.addPersonWithNewId(strbuf, 1);

      stb.append(tab);
      stb.append("<person id=\"");
      stb.append(id);
      stb.append("\">");
      stb.append(nl);

      if (p.m_stName != null) {
	  stb.append(tab2);
	  stb.append("<name>");
	  stb.append(p.m_stName);
	  stb.append("</name>");
	  stb.append(nl);
      }
      if (p.m_stEmail != null) {
	  stb.append(tab2);
	  stb.append("<emailaddress>");
	  stb.append(p.m_stEmail);
	  stb.append("</emailaddress>");
	  stb.append(nl);
      }
      if (p.m_stPhone != null) {
	  stb.append(tab2);
	  stb.append("<phone>");
	  stb.append(p.m_stPhone);
	  stb.append("</phone>");
	  stb.append(nl);
      }
      if (p.m_address.m_stStreet != null) {
	  stb.append(tab2);
	  stb.append("<address>");
	  stb.append(nl);

	  stb.append(tab3);
	  stb.append("<street>");
	  stb.append(p.m_address.m_stStreet);
	  stb.append("</street>");
	  stb.append(nl);

	  stb.append(tab3);
	  stb.append("<city>");
	  stb.append(p.m_address.m_stCity);
	  stb.append("</city>");
	  stb.append(nl);

	  stb.append(tab3);
	  stb.append("<country>");
	  stb.append(p.m_address.m_stCountry);
	  stb.append("</country>");
	  stb.append(nl);
	  
	  if (p.m_address.m_stProvince != null) {
	      stb.append(tab3);
	      stb.append("<province>");
	      stb.append(p.m_address.m_stProvince);
	      stb.append("</province>");
	      stb.append(nl);
	  }
	  stb.append(tab2);
	  stb.append("</address>");
	  stb.append(nl);
      }
      if (p.m_stHomepage != null) {
	  stb.append(tab2);
	  stb.append("<homepage>");
	  stb.append(p.m_stHomepage);
	  stb.append("</homepage>");
	  stb.append(nl);
      }
      if (p.m_stCreditcard != null) {
	  stb.append(tab2);
	  stb.append("<creditcard>");
	  stb.append(p.m_stCreditcard);
	  stb.append("</creditcard>");
	  stb.append(nl);
      }
      
      if (p.m_profile.m_stBusiness != null) {
	  stb.append(tab2);
	  stb.append("<profile income=\"");
	  stb.append(p.m_profile.m_stIncome);
	  stb.append("\">");
	  stb.append(nl);

	  for (int i=0; i < p.m_profile.m_vctInterest.size(); i++) {
	      stb.append(tab3);
	      stb.append("<interest category=\"");
	      stb.append(p.m_profile.m_vctInterest.get(i));
	      stb.append("\"/>");
	      stb.append(nl);
	  }
	  if (p.m_profile.m_stEducation != null) {
	      stb.append(tab3);
	      stb.append("<education>");
	      stb.append(p.m_profile.m_stEducation);
	      stb.append("</education>");
	      stb.append(nl);
	  }
	  if (p.m_profile.m_stGender != null) {
	      stb.append(tab3);
	      stb.append("<gender>");
	      stb.append(p.m_profile.m_stGender);
	      stb.append("</gender>");
	      stb.append(nl);
	  }

	  stb.append(tab3);
	  stb.append("<business>");
	  stb.append(p.m_profile.m_stBusiness);
	  stb.append("</business>");
	  stb.append(nl);

	  if (p.m_profile.m_stAge != null) {
	      stb.append(tab3);
	      stb.append("<age>");
	      stb.append(p.m_profile.m_stAge);
	      stb.append("</age>");
	      stb.append(nl);
	  }
	  stb.append(tab2);
	  stb.append("</profile>");
	  stb.append(nl);
      }
      if (p.m_vctWatches.size() != 0) {
	  stb.append(tab2);
	  stb.append("<watches>");
	  stb.append(nl);
	  for (int i=0; i<p.m_vctWatches.size(); i++) {
	      stb.append(tab3);
	      stb.append("<watch>");
	      stb.append(p.m_vctWatches.get(i));
	      stb.append("</watch>");
	      stb.append(nl);
	  }
	  stb.append(tab2);
	  stb.append("</watches>");
	  stb.append(nl);
      }

      stb.append(tab);
      stb.append("</person>");
      stb.append(nl);
    }
    stb.append("</people>");
    stb.append(nl);
  }

    private void writeTrace(StringBuffer stb) {
	long ts = System.currentTimeMillis();

	//The trace contains a unique ID, and a timestamp, so that
	// we can determine latency at the client
	try {
	    wrtTrace.write(String.valueOf(idTrace));
	    wrtTrace.write(",");
	    wrtTrace.write(String.valueOf(ts));
	    wrtTrace.write("\n");
	} catch (java.io.IOException ex) {
	    System.out.println("Write to gen trace file failed: " +
			       ex.getMessage());
	}
	stb.append(" id=\"");
	stb.append(idTrace++);
	stb.append("\" ts=\"");
	stb.append(ts);
	stb.append("\"");
    }
}

class AuctionValues extends HandlerBase {
  private String m_stFile = null;
  private ArrayList m_vctItems;
  private int m_nNextItemId = 0;
  private ArrayList m_vctPersons;
  private int m_nNextPersonId = 0;
  private ArrayList m_vctOpenAuctions;
  private int m_nNextOpenAuctionId = 0;
  private ArrayList m_vctCategories;
  private int m_nNextCategoryId = 0;
  private Calendar m_cal;
  private Random m_rnd = new Random();
  private boolean m_fInBid = false, m_fInBidDate = false;
  
  //Max wait between bids is 37 minutes
  private static final int MAXSECONDS = 2220;
  
  private StringBuffer stb;
  
  public AuctionValues() {
    stb = new StringBuffer();
    clear();
  }
  
  private void clear() {
    m_vctItems = new ArrayList();
    m_vctPersons = new ArrayList();
    m_vctOpenAuctions = new ArrayList();
    m_vctCategories = new ArrayList();
  }
  
  public int getItemCount() {
    return m_vctItems.size();
  }
  public String getItem(int i) {
    return (String) m_vctItems.get(i);
  }
  public String addItem(String stItemId) {
    int id = getId(stItemId);
    StringBuffer stb = new StringBuffer(stItemId);
    
    m_nNextItemId = Math.max(id, m_nNextItemId) + 1;
    
    if (id == 0) {
      id = m_nNextItemId;
      stb.append(m_nNextItemId);
    }
    
    m_vctItems.add(stb.toString());
    return stb.toString();
  }

  public void addItemWithNewId(StringBuffer stb, int howMany) {
      for (int i = 0; i < howMany; i++) {
	  stb.setLength(0);
	  stb.append("item");

	  m_nNextItemId++;
	  stb.append(m_nNextItemId);
	  m_vctItems.add(stb.toString());
      }
  }
  
  public int getPersonCount() {
    return m_vctPersons.size();
  }
  public String getPerson(int i) {
    return (String) m_vctPersons.get(i);
  }
  
  public String addPerson(String stPersonId) {
    StringBuffer stb = new StringBuffer(stPersonId);
    int id = getId(stPersonId);
    
    m_nNextPersonId = Math.max(id, m_nNextPersonId) + 1;

    if (id == 0) {
      id = m_nNextPersonId;
      stb.append(m_nNextPersonId);
    }

    m_vctPersons.add(stb.toString());
    return stb.toString();
  }
  
  public String addPersonWithNewId(StringBuffer stb, int howMany) {
      String person = null;
      for (int i = 0; i < howMany; i++) {
	  stb.setLength(0);
	  stb.append("person");
	  m_nNextPersonId++;
	  stb.append(m_nNextPersonId);
	  person = stb.toString();
	  m_vctPersons.add(person);
      }
      return person;
  }  
  
  public int getOpenAuctionCount() {
    return m_vctOpenAuctions.size();
  }
  public String getOpenAuction(int i) {
    return (String) m_vctOpenAuctions.get(i);
  }
  public String addOpenAuction(String stOAId) {
    int id = getId(stOAId);
    StringBuffer stb = new StringBuffer(stOAId);
    
    m_nNextOpenAuctionId = Math.max(id, m_nNextOpenAuctionId) + 1;
    
    if (id == 0) {
      id = m_nNextOpenAuctionId;
      stb.append(m_nNextOpenAuctionId);
    }
    
    m_vctOpenAuctions.add(stb.toString());
    return stb.toString();
  }

  public void addOpenAuctionWithNewId(StringBuffer stb, int howMany) {
      for (int i = 0; i < howMany; i++) {
	  stb.setLength(0); stb.append("open_auction");

	  m_nNextOpenAuctionId++;
	  
	  stb.append(m_nNextOpenAuctionId);
	  m_vctOpenAuctions.add(stb.toString());
      }
  }
  
  public int getCategoryCount() {
    return m_vctCategories.size();
  }
  public String getCategory(int i) {
    return (String) m_vctCategories.get(i);
  }

  public void addCategory(String stCategoryId) {
    int id = getId(stCategoryId);
    StringBuffer stb = new StringBuffer(stCategoryId);
    
    m_nNextCategoryId = Math.max(id, m_nNextCategoryId) + 1;
    
    if (id == 0) {
      id = m_nNextCategoryId;
      stb.append(m_nNextCategoryId);
    }
    
    m_vctCategories.add(stb.toString());
  }

  public void addCategoryWithNewId(StringBuffer stb, int howMany) {
      for (int i = 0; i < howMany; i++) {
	  stb.setLength(0); stb.append("category");
	  m_nNextCategoryId++;
	  stb.append(m_nNextCategoryId);
	  m_vctCategories.add(stb.toString());
      }
  }
  
    // XXX vpapad: this method makes baby Jesus cry
  public int getId(String st) {
    int id = 0, i = 0;
    
    while (id == 0 && i < st.length()) {
      try{
	id = Integer.parseInt(st.substring(i));
      } catch (Exception e) {
	;
      }
      i++;
    }
    
    return id;
  }
  
  public Date getCurrentTime() {
    m_cal.add(Calendar.SECOND, m_rnd.nextInt(MAXSECONDS));
    
    return m_cal.getTime();
  }
  
  public void init(String stFile, Calendar cal) {
    if (m_stFile == stFile)
      return;
    
    m_cal = cal;
    clear();
    
    m_stFile = stFile;
    if (stFile == null || stFile.length() == 0) {
      System.out.println("No xmark file specified," +
			 " using default values...");
      defaultValues();
    } else {
      try {
	  SAXParserFactory factory = SAXParserFactory.newInstance();
	  SAXParser sp = factory.newSAXParser();
	  sp.parse(stFile, this);
      } catch (org.xml.sax.SAXException ex) {
	System.out.println("SAX Error: '" + ex.getMessage() + "'");
	System.out.println("Using default values...");
	defaultValues();
      } catch (java.io.IOException ex) {
	System.out.println("IO Error: '" + ex.getMessage() + "'");
	System.out.println("Using default values...");
	defaultValues();
      } catch(javax.xml.parsers.ParserConfigurationException pce) {
	  System.out.println("Parser Config Error: '" + pce.getMessage() 
			     + "'");
      }
    }
  }
  
  private void defaultValues() {
    addPersonWithNewId(stb, 2); 
    addItemWithNewId(stb, 2);
    addOpenAuctionWithNewId(stb, 1);
    addCategoryWithNewId(stb, 1);
  }
  
  public void startElement (String name, AttributeList attrs)
    throws SAXException {
    
    if (m_fInBid == true) {
      if (name.compareToIgnoreCase("date") == 0)
	m_fInBidDate = true;
    }
    
    if (name.compareToIgnoreCase("person") == 0) {
      //New person, add them to the list
      addPerson(attrs.getValue("id"));
    } else if (name.compareToIgnoreCase("item") == 0) {
      //New item, add it to the list
      addItem(attrs.getValue("id"));
    } else if (name.compareToIgnoreCase("open_auction") == 0) {
      //New open, add it to the list
      addOpenAuction(attrs.getValue("id"));
    } else if (name.compareToIgnoreCase("category") == 0) {
      //New open, add it to the list
      addCategory(attrs.getValue("id"));
    } else if (name.compareToIgnoreCase("bidder") == 0) {
      m_fInBid = true;
    }
  }
  
  public void endElement (String name) throws SAXException {
    if (name.compareToIgnoreCase("date") == 0) {
      m_fInBidDate = false;
    } else if (name.compareToIgnoreCase("bidder") == 0) {
      m_fInBid = false;
    }
  }
  
  public void characters (char buf [], int offset, int len)
    throws SAXException {
    
    if (m_fInBidDate) {
      SimpleDateFormat sdfDate = new SimpleDateFormat("MM/dd/yyyy");
      String st = new String(buf, offset, len);
      
      try {
	Date dtNew = sdfDate.parse(st);
	if (dtNew.after(m_cal.getTime())) {
	  m_cal.setTime(dtNew);
	  m_cal.add(Calendar.DATE, 1);
	}
      } catch (java.text.ParseException ex) {
	;
      }
    }
  }
}

class Person {
  class Profile {
    public ArrayList m_vctInterest = new ArrayList();
    public String m_stEducation;
    public String m_stGender;
    public String m_stBusiness;
    public String m_stAge;
    public String m_stIncome;
  }
  class Address {
    public String m_stStreet;
    public String m_stCity;
    public String m_stProvince;
    public String m_stCountry;
    public String m_stZipcode;
  }

  public String m_stName;
  public String m_stEmail;
  public String m_stPhone;
  public Address m_address = new Address();
  public String m_stHomepage;
  public String m_stCreditcard;
  public Profile m_profile = new Profile();
  public ArrayList m_vctWatches = new ArrayList();

  private static Random m_rnd;

  public void init(AuctionValues av) {
    if (m_rnd == null)
      m_rnd = new Random();

    int ifn = m_rnd.nextInt(ValueGen.firstnames.length);
    int iln = m_rnd.nextInt(ValueGen.lastnames.length);
    int iem = m_rnd.nextInt(ValueGen.emails.length);

    this.m_stName = new String(ValueGen.firstnames[ifn] + " " +
			       ValueGen.lastnames[iln]);
    this.m_stEmail = new String(ValueGen.lastnames[iln] + "@" +
				ValueGen.emails[iem]);
    if (m_rnd.nextBoolean()) {
      this.m_stPhone = new String("+" + (m_rnd.nextInt(98) + 1) +
				  " (" + (m_rnd.nextInt(989) + 10) +
				  ") " + (m_rnd.nextInt(9864196) + 123457));
    }
    if (m_rnd.nextBoolean()) {
      genAddress();
    }
    if (m_rnd.nextBoolean()) {
      this.m_stHomepage = new String("http://www." + ValueGen.emails[iem] +
				     "/~" + ValueGen.lastnames[iln]);
    }
    if (m_rnd.nextBoolean()) {
      this.m_stCreditcard = new String((m_rnd.nextInt(9000) + 1000) + " " + 
				       (m_rnd.nextInt(9000) + 1000) + " " + 
				       (m_rnd.nextInt(9000) + 1000) + " " + 
				       (m_rnd.nextInt(9000) + 1000));
    }
    if (m_rnd.nextBoolean()) {
      genProfile(av);
    }
    if (m_rnd.nextBoolean()) {
      int cWatches = m_rnd.nextInt(20) + 1;
      int iWatch;
      for (int i=0; i<cWatches; i++) {
	iWatch = m_rnd.nextInt(av.getOpenAuctionCount());
	m_vctWatches.add(av.getOpenAuction(iWatch));
      }
    }
  }

  private void genAddress() {
    int ist = m_rnd.nextInt(ValueGen.lastnames.length);
    int ict = m_rnd.nextInt(ValueGen.cities.length);
    int icn = (m_rnd.nextInt(4) != 0) ? 0 :
      m_rnd.nextInt(ValueGen.countries.length);
    int ipv = (icn == 0) ? m_rnd.nextInt(ValueGen.provinces.length) :
      m_rnd.nextInt(ValueGen.lastnames.length);

    this.m_address.m_stStreet = new String((m_rnd.nextInt(99) + 1) + " " +
					   ValueGen.lastnames[ist] + " St");
    this.m_address.m_stCity = ValueGen.cities[ict];
    if (icn == 0) {
      this.m_address.m_stCountry = "United States";
      this.m_address.m_stProvince = ValueGen.provinces[ipv];
    } else {
      this.m_address.m_stCountry = ValueGen.countries[icn];
    }
    this.m_address.m_stZipcode = String.valueOf(m_rnd.nextInt(99999) + 1);
  }

  private void genProfile(AuctionValues av) {
    if (m_rnd.nextBoolean())
      this.m_profile.m_stEducation =
	ValueGen.education[m_rnd.nextInt(ValueGen.education.length)];

    if (m_rnd.nextBoolean())
      this.m_profile.m_stGender = (m_rnd.nextBoolean()) ? "male" : "female";

    this.m_profile.m_stBusiness = (m_rnd.nextBoolean()) ? "Yes" : "No";

    if (m_rnd.nextBoolean())
      this.m_profile.m_stAge = String.valueOf((m_rnd.nextInt(15) + 30));

    this.m_profile.m_stIncome = new String((m_rnd.nextInt(30000) + 40000) +
					 "." + (m_rnd.nextInt(99)));

    int cInterest = m_rnd.nextInt(25);
    int iCategory;
    for (int i = 0; i < cInterest; i++) {
      iCategory = m_rnd.nextInt(av.getCategoryCount());
      this.m_profile.m_vctInterest.add(av.getCategory(iCategory));
    }
  }
}

class ValueGen {
  public static final String firstnames[]={
    "Frederique","Shigeichiro","Xinan","Takahira","Rildo","IEEE","Weiru",
    "Nitsan","Taiji","Takahiro","Zsolt","Xiaoheng","Toney","Tru","Nishit",
    "Gudjon","Satoru","Mohd","Golgen","Nidapan","Lidong","Serap",
    "Domenick","Woody","Ebbe","Tse","Domenico","Zeydy","Hidde","Fumiko",
    "Sajjad","Satosi","Hitofumi","Sibyl","Mechthild","Pramod","Eishiro",
    "Demin","Sajjan","Jinpo","Kazuyasu","Lijia","Branimir","Lijie","Moie",
    "Yoshimitsu","Tsz","Berhard","Clyde","Shakhar","Moriyoshi","Khedija",};

  public static final String lastnames[]={
    "Wossner","Gunderson","Comte","Linnainmaa","Harbusch","Speek",
    "Trachtenberg","Kohling","Speel","Nollmann","Jervis","Capobianchi",
    "Murillo","Speer","Claffy","Lalonde","Nitta","Servieres","Chimia",
    "Boreale","Taubenfeld","Nitto","Walston","Danley","Billawala",
    "Ratzlaff","Penttonen","Pashtan","Iivonen","Setlzner","Reutenauer",
    "Hegner","Demir","Ramaiah","Covnot","Nitsch","Thummel","Axelband",
    "Sevcikova","Shobatake","Greibach","Fujisaki","Bugrara","Dolinsky",
    "Dichev","Versino","Gluchowski","Dahlbom","Suri","Parveen","Businaro",
    "Taneja","Morrey","Siochi","Alameldin","Genin","McAlpine","Sury",
    "Angel","Sambasivam","Bazelow","Demke","Anger","Brendel",
    "Cappelletti","Walstra","Hebden","Carrera","Brender","Carrere",
    "Kalloufi","Katzenelson","Jeansoulin","Renear","Zuberek","Snyers",};

  public static final String cities[]={
    "Abidjan","Abu","Acapulco","Aguascalientes","Akron","Albany",
    "Albuquerque","Alexandria","Allentown","Amarillo","Amsterdam",
    "Anchorage","Appleton","Aruba","Asheville","Athens","Atlanta",
    "Augusta","Austin","Baltimore","Bamako","Bangor","Barbados",
    "Barcelona","Basel","Baton","Beaumont","Berlin","Bermuda","Billings",
    "Birmingham","Boise","Bologna","Boston","Bozeman","Brasilia",
    "Brunswick","Brussels","Bucharest","Budapest","Buffalo","Butte",
    "Cairo","Calgary","Cancun","Cape","Caracas","Casper","Cedar",
    "Charleston","Charlotte","Charlottesville","Chattanooga","Chicago",
    "Chihuahua","Cincinnati","Ciudad","Cleveland","Cody","Colorado",
    "Columbia","Columbus","Conakry","Copenhagen","Corpus","Cozumel",
    "Dakar","Dallas","Dayton","Daytona","Denver","Des","Detroit","Dothan",
    "Dubai","Dublin","Durango","Durban","Dusseldorf","East","El","Elko",
    "Evansville","Fairbanks","Fayetteville","Florence","Fort","Fortaleza",
    "Frankfurt","Fresno","Gainesville","Geneva","George","Glasgow",
    "Gothenburg","Grand","Great","Green","Greensboro","Greenville",
    "Grenada","Guadalajara","Guangzhou","Guatemala","Guaymas","Gulfport",
    "Gunnison","Hamburg","Harrisburg","Hartford","Helena","Hermosillo",
    "Honolulu","Houston","Huntington","Huntsville","Idaho","Indianapolis",
    "Istanbul","Jackson","Jacksonville","Johannesburg","Kahului",
    "Kalamazoo","Kalispell","Kansas","Key","Kiev","Killeen","Knoxville",
    "La","Lafayette","Lansing","Las","Lawton","Leon","Lexington","Lima",
    "Lisbon","Little","Lome","London","Long","Lorient","Los","Louisville",
    "Lubbock","Lynchburg","Lyon","Macon","Madison","Madrid","Manchester",
    "Mazatlan","Melbourne","Memphis","Merida","Meridian","Mexico","Miami",
    "Milan","Milwaukee","Minneapolis","Missoula","Mobile","Monroe",
    "Monterrey","Montgomery","Montreal","Moscow","Mulhouse","Mumbai",
    "Munich","Myrtle","Nagoya","Nashville","Nassau","New","Newark",
    "Newburgh","Newcastle","Nice","Norfolk","Oakland","Oklahoma","Omaha",
    "Ontario","Orange","Orlando","Ouagadougou","Palm","Panama","Paris",
    "Pasco","Pensacola","Philadelphia","Phoenix","Pittsburgh","Pocatello",
    "Port","Portland","Porto","Prague","Providence","Providenciales",
    "Puebla","Puerto","Raleigh","Rapid","Reno","Richmond","Rio","Roanoke",
    "Rochester","Rome","Sacramento","Salt","Salvador","San","Santiago",
    "Sao","Sarasota","Savannah","Seattle","Shannon","Shreveport","South",
    "Spokane","St","Stockholm","Stuttgart","Sun","Syracuse","Tallahassee",
    "Tampa","Tapachula","Texarkana","Tokyo","Toledo","Toronto","Torreon",
    "Toulouse","Tri","Tucson","Tulsa","Turin","Twin","Vail","Valdosta",
    "Vancouver","Venice","Veracruz","Vienna","Villahermosa","Warsaw",
    "Washington","West","White","Wichita","Wilkes","Wilmington",
    "Windhoek","Worcester","Zihuatenejo","Zurich"
};

  public static final String countries[]={
    "United States","Afghanistan","Albania","Algeria","American Samoa",
    "Andorra","Angola","Anguilla","Antarctica","Antigua","Argentina",
    "Armenia","Aruba","Australia","Austria","Azerbaijan","Bahamas",
    "Bahrain","Bangladesh","Barbados","Belarus","Belgium","Belize",
    "Benin","Bermuda","Bhutan","Bolivia","Botswana","Brazil",
    "British Indian Ocean Territory","British Virgin Islands",
    "Brunei Darussalam","Bulgaria","Burkina Faso","Burundi",
    "Cacos Islands","Cambodia","Cameroon","Canada","Cape Verde",
    "Cayman Islands","Central African Republic","Chad","Chile","China",
    "Christmas Island","Colombia","Comoros","Congo","Cook Islands",
    "Costa Rica","Croatia","Cuba","Cyprus","Czech Republic","Denmark",
    "Djibouti","Dominica","Dominican Republic","East Timor","Ecuador",
    "Egypt","El Salvador","Equatorial Guinea","Eritrea","Estonia",
    "Ethiopia","Falkland Islands","Faroe Islands","Fiji","Finland",
    "France","French Guiana","French Polynesia",
    "French Southern Territory","Futuna Islands","Gabon","Gambia",
    "Georgia","Germany","Ghana","Gibraltar","Greece","Greenland",
    "Grenada","Guadeloupe","Guam","Guatemala","Guinea","Guyana","Haiti",
    "Heard and Mcdonald Island","Honduras","Hong Kong","Hungary",
    "Iceland","India","Indonesia","Iran","Iraq","Ireland","Israel",
    "Italy","Ivory Coast","Jamaica","Japan","Jordan","Kazakhstan","Kenya",
    "Kiribati","Korea, Democratic People's Rep","Korea, Republic Of",
    "Kuwait","Kyrgyzstan","Lao People's Democratic Republ","Latvia",
    "Lebanon","Lesotho","Liberia","Libyan Arab Jamahiriya","Lithuania",
    "Luxembourg","Macau","Macedonia","Madagascar","Malawi","Malaysia",
    "Maldives","Mali","Malta","Marshall Islands","Martinique",
    "Mauritania","Mauritius","Mayotte","Mexico","Micronesia",
    "Moldova, Republic Of","Monaco","Mongolia","Montserrat","Morocco",
    "Mozambique","Myanmar","Namibia","Nauru","Nepal","Netherlands",
    "Netherlands Antilles","New Caledonia","New Zealand","Nicaragua",
    "Niger","Nigeria","Niue","Norfolk Island","Northern Mariana Islands",
    "Norway","Oman","Pakistan","Palau","Panama","Papua New Guinea",
    "Paraguay","Peru","Philippines","Poland","Portugal","Puerto Rico",
    "Qatar","Reunion","Romania","Russian Federation","Rwanda",
    "Saint Kitts","Samoa","San Marino","Sao Tome","Saudi Arabia",
    "Senegal","Seychelles","Sierra Leone","Singapore","Slovakia",
    "Slovenia","Solomon Islands","Somalia","South Africa","South Georgia",
    "Spain","Sri Lanka","St. Helena","St. Lucia","St. Pierre",
    "St. Vincent and Grenadines","Sudan","Suriname",
    "Svalbard and Jan Mayen Island","Swaziland","Sweden","Switzerland",
    "Syrian Arab Republic","Taiwan","Tajikistan","Tanzania","Thailand",
    "Togo","Tokelau","Tonga","Trinidad","Tunisia","Turkey","Turkmenistan",
    "Turks Islands","Tuvalu","Uganda","Ukraine","United Arab Emirates",
    "United Kingdom","Uruguay","Us Minor Islands","Us Virgin Islands",
    "Uzbekistan","Vanuatu","Vatican City State","Venezuela","Viet Nam",
    "Western Sahara","Yemen","Zaire","Zambia","Zimbabwe"};

  public static final String emails[]={
    "ab.ca","ac.at","ac.be","ac.jp","ac.kr","ac.uk","acm.org",
    "airmail.net","arizona.edu","ask.com","att.com","auc.dk","auth.gr",
    "baylor.edu","bell-labs.com","bellatlantic.net","berkeley.edu",
    "brandeis.edu","broadquest.com","brown.edu","cabofalso.com","cas.cz",
    "clarkson.edu","clustra.com","cmu.edu","cnr.it","co.in","co.jp",
    "cohera.com","columbia.edu","compaq.com","computer.org",
    "concentric.net","conclusivestrategies.com","concordia.ca",
    "cornell.edu","crossgain.com","csufresno.edu","cti.gr","cwi.nl",
    "cwru.edu","dauphine.fr","dec.com","du.edu","duke.edu",
    "earthlink.net","edu.au","edu.cn","edu.hk","edu.sg","emc.com",
    "ernet.in","evergreen.edu","fernuni-hagen.de","filelmaker.com",
    "filemaker.com","forth.gr","forwiss.de","fsu.edu","gatech.edu",
    "gmu.edu","gte.com","hitachi.com","hp.com","ibm.com","imag.fr",
    "indiana.edu","infomix.com","informix.com","inria.fr","intersys.com",
    "itc.it","labs.com","lante.com","lbl.gov","lehner.net","llnl.gov",
    "lri.fr","lucent.com","memphis.edu","microsoft.com","mit.edu",
    "mitre.org","monmouth.edu","msn.com","msstate.edu","ncr.com",
    "neu.edu","newpaltz.edu","njit.edu","nodak.edu","ntua.gr","nwu.edu",
    "nyu.edu","ogi.edu","okcu.edu","oracle.com","ou.edu","panasonic.com",
    "pi.it","pitt.edu","poly.edu","poznan.pl","prc.com","propel.com",
    "purdue.edu","rice.edu","rpi.edu","rutgers.edu","rwth-aachen.de",
    "savera.com","sbphrd.com","sds.no","sdsc.edu","sfu.ca",
    "sleepycat.com","smu.edu","solidtech.com","stanford.edu","sun.com",
    "sunysb.edu","sybase.com","telcordia.com","temple.edu","toronto.edu",
    "tue.nl","twsu.edu","ualberta.ca","ubs.com","ucd.ie","ucdavis.edu",
    "ucf.edu","ucla.edu","ucr.edu","ucsb.edu","ucsd.edu","ufl.edu",
    "uga.edu","uic.edu","uiuc.edu","ul.pt","umass.edu","umb.edu",
    "umd.edu","umich.edu","umkc.edu","unbc.ca","unf.edu",
    "uni-freiburg.de","uni-mannheim.de","uni-marburg.de","uni-mb.si",
    "uni-muenchen.de","uni-muenster.de","uni-sb.de","uni-trier.de",
    "unical.it","unizh.ch","unl.edu","upenn.edu","uqam.ca","uregina.ca",
    "usa.net","ust.hk","uta.edu","utexas.edu","uu.se","uwaterloo.ca",
    "uwindsor.ca","uwo.ca","verity.com","versata.com","washington.edu",
    "whizbang.com","wisc.edu","wpi.edu","yahoo.com","yorku.ca",
    "zambeel.com"};

  public static final String provinces[]={
    "Alabama","Alaska","Arizona","Arkansas","California","Colorado",
    "Connecticut","Delaware","District Of Columbia","Florida","Georgia",
    "Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky",
    "Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota",
    "Mississipi","Missouri","Montana","Nebraska","Nevada","New Hampshire",
    "New Jersey","New Mexico","New York","North Carolina","North Dakota",
    "Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island",
    "South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont",
    "Virginia","Washington","West Virginia","Wisconsin","Wyoming"};

  public static final String education[]={
    "High School", "College", "Graduate School", "Other"};
}

