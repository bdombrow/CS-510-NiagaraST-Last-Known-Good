package niagara.firehose;

// NOT SYNCHRONIZED - KT

import java.util.*;
import java.nio.*;

class PersonGen {
    class Profile {
	public Vector m_vctInterest = new Vector();

	public boolean has_education;
	public boolean has_gender;
	public boolean has_age;

	public String m_stEducation;
	public String m_stGender;
	public String m_stBusiness;
	public String m_stAge;

	public CharBuffer m_stIncome = CharBuffer.allocate(30);
    }
    class Address {
	public CharBuffer m_stStreet = CharBuffer.allocate(100);
	public String m_stCity;
	public String m_stProvince;
	public String m_stCountry;
	public String m_stZipcode;
    }

    public boolean has_phone;
    public boolean has_address;
    public boolean has_homepage;
    public boolean has_creditcard;
    public boolean has_profile;
    public boolean has_watches;
    
    public CharBuffer m_stName = CharBuffer.allocate(100);
    public CharBuffer m_stEmail = CharBuffer.allocate(100);
    public CharBuffer m_stPhone = CharBuffer.allocate(15);
    public Address m_address = new Address();
    public CharBuffer m_stHomepage = CharBuffer.allocate(100);
    public CharBuffer m_stCreditcard = CharBuffer.allocate(20);
    public Profile m_profile = new Profile();
    public Vector m_vctWatches = new Vector();
    
    private Random m_rnd = new Random(20934);
    public static int NUM_CATEGORIES = 1000;

    
    public void generateValues(OpenAuctions auctions) {
	int ifn = m_rnd.nextInt(FirstnamesGen.NUM_FIRSTNAMES);
	int iln = m_rnd.nextInt(LastnamesGen.NUM_LASTNAMES);
	int iem = m_rnd.nextInt(BigValueGen.NUM_EMAILS);
	
	m_stName.clear();
	m_stName.put(FirstnamesGen.firstnames[ifn]);
	m_stName.put(" ");
	m_stName.put(LastnamesGen.lastnames[iln]);
	
	m_stEmail.clear();
	m_stEmail.put(LastnamesGen.lastnames[iln]);
	m_stEmail.put("@"); 
	m_stEmail.put(BigValueGen.emails[iem]);
		      
	if (m_rnd.nextBoolean()) {
	    has_phone = true;
	    m_stPhone.clear();
	    m_stPhone.put("+");
	    m_stPhone.put(IntToString.strings[m_rnd.nextInt(98)+1]);
	    m_stPhone.put("(");
	    m_stPhone.put(IntToString.strings[m_rnd.nextInt(989)+10]);
	    m_stPhone.put(")");
	    m_stPhone.put(String.valueOf(m_rnd.nextInt(9864196) + 123457));
	} else {
	    has_phone = false;
	}
	
	if (m_rnd.nextBoolean()) {
	    has_address = true;
	    genAddress();
	} else {
	    has_address = false;
	}
	
	if (m_rnd.nextBoolean()) {
	    has_homepage = true;
	    m_stHomepage.clear();
	    m_stHomepage.put("http://www.");
	    m_stHomepage.put(BigValueGen.emails[iem]);
	    m_stHomepage.put("/~");
	    m_stHomepage.put(LastnamesGen.lastnames[iln]);
	} else {
	    has_homepage = false;
	}
	
	if (m_rnd.nextBoolean()) {
	    has_creditcard = true;
	    m_stCreditcard.clear();
	    m_stCreditcard.put(String.valueOf(m_rnd.nextInt(9000) + 1000)); //HERE
	    m_stCreditcard.put(" ");
	    m_stCreditcard.put(String.valueOf(m_rnd.nextInt(9000) + 1000)); //HERE
	    m_stCreditcard.put(" ");
	    m_stCreditcard.put(String.valueOf(m_rnd.nextInt(9000) + 1000)); //HERE
	    m_stCreditcard.put(" ");
	    m_stCreditcard.put(String.valueOf(m_rnd.nextInt(9000) + 1000)); //HERE
	} else {
	    has_creditcard = false;
	}
      
	if (m_rnd.nextBoolean()) {
	    has_profile = true;
	    genProfile();
	} else {
	    has_profile = false;
	}
	
	has_watches = false;
	/* skip watches for now -  expensive and problem with
	 * people who are generated before any items
	if (m_rnd.nextBoolean()) {
	    int cWatches = m_rnd.nextInt(20) + 1;
	    int iWatch;
	    for (int i=0; i<cWatches; i++) {
		// is this OK, will this screw up bids/items distribution??
		m_vctWatches.add(String.valueOf(auctions.getExistingId()));
	    }
	} else {
	    m_vctWatches.clear();
	    }*/
    }
    
    private void genAddress() {
	int ist = m_rnd.nextInt(LastnamesGen.NUM_LASTNAMES); // street
	int ict = m_rnd.nextInt(BigValueGen.NUM_CITIES); // city
	int icn = (m_rnd.nextInt(4) != 0) ? 0 :
	    m_rnd.nextInt(BigValueGen.NUM_COUNTRIES); 
	int ipv = (icn == 0) ? m_rnd.nextInt(BigValueGen.NUM_PROVINCES) :
	    m_rnd.nextInt(LastnamesGen.NUM_LASTNAMES);  // provinces are really states
	
	m_address.m_stStreet.clear();
	m_address.m_stStreet.put(String.valueOf((m_rnd.nextInt(99) + 1))); 
	m_address.m_stStreet.put(" ");
	m_address.m_stStreet.put(LastnamesGen.lastnames[ist]);
	m_address.m_stStreet.put(" St");
	
	m_address.m_stCity = BigValueGen.cities[ict];
	
	if (icn == 0) {
	    m_address.m_stCountry = "United States";
	    m_address.m_stProvince = BigValueGen.provinces[ipv];
	} else {
	    m_address.m_stCountry = BigValueGen.countries[icn];
	    m_address.m_stProvince = LastnamesGen.lastnames[ipv];
	}
	m_address.m_stZipcode = String.valueOf(m_rnd.nextInt(99999) + 1);
    }

    private void genProfile() {
	if (m_rnd.nextBoolean()) {
	    m_profile.has_education = true;
	    m_profile.m_stEducation =
		BigValueGen.education[m_rnd.nextInt(BigValueGen.NUM_EDUCATION)];
	} else {
	    m_profile.has_education = false;
	}
	
	if (m_rnd.nextBoolean()) {
	    m_profile.has_gender = true;
	    m_profile.m_stGender = (m_rnd.nextInt(2) == 1) ? "male" : "female";
	} else {
	    m_profile.has_gender = false;
	}
	
	m_profile.m_stBusiness = (m_rnd.nextInt(2) == 1) ? "Yes" : "No";
	
	if (m_rnd.nextBoolean()) {
	    m_profile.has_age = true;
	    m_profile.m_stAge = IntToString.strings[m_rnd.nextInt(15) + 30]; // HERE
	} else {
	    m_profile.has_age = false;
	}
	
	// incomes are zipfian - change this if we start to use
	// income values KT
	m_profile.m_stIncome.clear();
	m_profile.m_stIncome.put(String.valueOf((m_rnd.nextInt(30000) + 40000)));
	m_profile.m_stIncome.put(".");
	m_profile.m_stIncome.put(IntToString.strings[m_rnd.nextInt(99)]); //  HERE
	
	int cInterest = m_rnd.nextInt(5);
	int iCategory;
	m_profile.m_vctInterest.setSize(0);
	for (int i = 0; i < cInterest; i++) {
	    // HERE
	    m_profile.m_vctInterest.add(String.valueOf(m_rnd.nextInt(NUM_CATEGORIES)));
	}
    }
}
