package niagara.optimizer;

/**
 LOGICAL PROPERTIES
*/
class LOG_PROP {
public: 
	LOG_PROP(){};
	public boolean isItemProp() {return false;};
	public boolean isCollProp() {return false;};
	virtual CString Dump()=0;
	virtual CString DumpCOVE()=0;

LOG_PROP::LOG_PROP(float card, float ucard, KEYS * keys, 
                   ATTRS * attrs, CStrings * free_attrs, ATTRS * mat_attrs)
    :Card(card),UCard(ucard), Keys(keys), Attrs(attrs),FreeAttrs(free_attrs), 
     FKeys(null)
{
	assert(Card>=0);
	assert(UCard>=0);

	if (keys == null) Keys = new KEYS;
	
	if (attrs == null) Attrs = new ATTRS;
	
	if (free_attrs == null) FreeAttrs = new CStrings;
	

	PathExps = new CStrings;

	AttrProps = new ATTR_PROPS;

	if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_LOG_PROP]->New(); 

        // By default, things are local
        local = true;
};

//Initialize Log_Prop with a Get Operator
// - logprop is from a base collection, e.g., Depts
// - attr is the variable name the Get operator output,
//   e.g., <name:d, domain:Department>

LOG_PROP::LOG_PROP(LOG_PROP * LogProp, DOM * Domain, CString varname)
: Card(LogProp->GetCard()), UCard(LogProp->GetUCard())
{
	assert(Card>=0);
	assert(UCard>=0);
	// The attribute named with "varname"
	ATTR * Attr = new ATTR(varname, Domain);

	Attrs = new ATTRS;
	Attrs->Add(Attr);		// attr is the only attribute at this point
	
	Keys = new KEYS;
	
	Keys ->Add(new KEY(varname)); //Add this attribute as key.  
	
	FKeys = new KEYS;
		
	FreeAttrs = new CStrings;

	// The path expression in the base table do not 
	// have variable name as prefix
	// Add prefix, i.e., var, to all the path expressions 

	FDs = new FDS;

	PathExps = new CStrings;
	
	AttrProps = new ATTR_PROPS;

	for (uint i=0; i<LogProp->GetPathExps()->size(); i++) {
	

		CString pathexp = LogProp->GetPathExps()->GetAt(i);
		
		if (pathexp == "") 
			PathExps->push_back(varname);		// "" --> "e"
		else 
			PathExps->push_back(varname+"."+ pathexp);	// dept.name --> e.dept.name
		
		AttrProps->push_back(LogProp->GetAttrProps()->GetAt(i));
	}

        local = LogProp->isLocal();

	if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_LOG_PROP]->New(); 
	
	//Indices->Copy(coll->Indices);			//Index informatioin is only used
	//BitIndices->Copy(coll->BitIndices);	//by Get, and can be obtained for COLL
	// Order = other.GetOrder(); // order is physical 	
};

//Initialize Log_Prop with a Get_CVA Operator
// - inputprop is for the host collection, e.g., Depts
// - cva is the name of the CVA, for instance, dept.Students 
// - attr is the attribute output by the Get_CVA operator
//   for instance, Get_CVA(dept.Students) generate attribute
//   <name: a. domain:Student>

LOG_PROP::LOG_PROP(LOG_PROP * hostprop, DOM * hostdom, CString cvaname, CString varname)
{
	// Derive "Students" from "facultymember.dept.Students"
	CString cva = cvaname;
	int n=cva.ReverseFind('.'); // The position of the first . to left
	cva.Delete(0, n+1); 

	// The attribute named with "varname"
	ATTR * Attr = new ATTR(varname, hostdom->GetDom(cva));
	Attrs = new ATTRS;
	Attrs->Add(Attr);

	Keys = new KEYS;

	FKeys = new KEYS;  

	FreeAttrs = new CStrings;
	FreeAttrs->push_back(cvaname);	// a dependent attribute

	// Retrive the CVA_PROP for "cva"
	CVA_PROP *cvaprop = (CVA_PROP*) hostprop->GetAttrProp(cva);
	Card = cvaprop->GetCard();
	UCard = cvaprop->GetUCard();
	//GUCard = cvaprop->GetGUCard();
	//InstUCard = cvaprop->GetInstUCard();
	assert(Card>=0);
	assert(UCard>=0);


	FDs = new FDS;

	// For attributes of CVA, the path expressions in log_prop
	// store the whole path such as "dept.Students.name"
	// Need to replace the prefix with cva, such as "s.name"
	PathExps = new CStrings;
	AttrProps = new ATTR_PROPS;
	for (uint i=0; i< hostprop->GetPathExps()->size(); i++) {
		CString pe = hostprop->GetPathExps()->GetAt(i);		
	// If the path expression prefix with cva, replace the prefix 
		if (pe.Find(cva)==0) 
			pe.Replace(cva, varname);
		PathExps->push_back(pe);
		AttrProps->push_back(hostprop->GetAttrProps()->GetAt(i));
	}
	if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_LOG_PROP]->New(); 

};// LOG_PROP::LOG_PROP(ATTR_CVA * , Var *)

LOG_PROP::LOG_PROP(LOG_PROP& other)				// copy constructor
{
	Card = other.GetCard();
	UCard = other.GetUCard();
	
	assert(Card>=0);
	assert(UCard>=0);

	Keys = (KEYS *)other.GetKeys()->Copy();
	FKeys = (KEYS *)other.GetFKeys()->Copy();
	
	//FKeys = new KEYS();
	//for (i=0; i<other.FKeys->size(); i++)
		//FKeys->Add(new FKEY(*(FKEY*)other.FKeys->GetAt(i)));
		
	Attrs = new ATTRS();
	for (uint i=0; i<other.GetAttrs()->size(); i++)
		Attrs->push_back(other.GetAttrs()->GetAt(i)->Copy());
	
	FreeAttrs = new CStrings();
	for (uint i=0; i<other.GetFreeAttrs()->size(); i++)
		FreeAttrs->push_back(other.GetFreeAttrs()->GetAt(i));
	
	FDs = (FDS*)other.GetFDs()->Copy();

	PathExps = new CStrings;	
	AttrProps = new ATTR_PROPS;
	for (uint i=0; i<other.GetPathExps()->size(); i++) {
		PathExps->push_back(other.GetPathExps()->GetAt(i));
		AttrProps->push_back(other.GetAttrProps()->GetAt(i)->Copy());
	}

        local = other.local;
};

// Return the number of bytes each element takes. 
int LOG_PROP::GetPhyWidth() {
    int width = 0;
    for (uint i =0; i<Attrs->size(); i++) {
        width += Attrs->GetAt(i)->GetIDWidth();
    }
    // assert(width>0); // XXX 
    // XXX Vassilis: It seems to me that the Dept LOG_PROP in cat.cpp 
    // XXX cannot possibly satisfy this assertion - we never add any
    // XXX attributes to it, only attribute properties...
    
    return width;
}
	CStrings * LOG_PROP::GetAttrNames() { return Attrs->GetAttrNames(); };
	ATTR * LOG_PROP::GetAttr(int i) { return Attrs->GetAt(i); };
	ATTR * LOG_PROP::GetAttr(CString attrname) { return Attrs->GetAttr(attrname); };
	KEY * LOG_PROP::GetKey(int i) { return Keys->GetAt(i); };
	KEY * LOG_PROP::GetFKey(int i) { return FKeys->GetAt(i); };
	KEY * LOG_PROP::GetPrimKey() { 
		return Keys -> GetAt(0); 
	};
	
	bool LOG_PROP::Contains(ATTR * attr) { return Attrs->Contains(attr); };

	bool LOG_PROP::Contains(ATTRS * attrs) { return Attrs->Contains(attrs); };
	
	bool LOG_PROP::Contains(CString attrname) { return Attrs->Contains(attrname); };
	
	bool LOG_PROP::Contains(CStrings * attrnames) { return Attrs->Contains(attrnames); };

	int LOG_PROP::GetDegree() { return Attrs -> size();}; // Degree is the number of attributes 
	
LOG_PROP::~LOG_PROP() 
{ 
	// Be aware that this may delete attributes that are used by other 
	// log_coll_prop.
//	for(int i=0;i<Keys->size();i++) delete Keys->GetAt(i);
//	for(i=0;i<FKeys->size();i++) delete FKeys->GetAt(i);
//	for(i=0;i<MatAttrs->size();i++) delete MatAttrs->GetAt(i);
//	for(i=0;i<FreeAttrs->size();i++) delete FreeAttrs->GetAt(i);
	if (TraceOn && !ForGlobalEpsPruning) ClassStat[C_LOG_PROP]->Delete();
	
};

// LOG_PROP::Restrictivity
// Compute the restriction factor if nested or projected on "AttrNames"
// This Funciton is used in
//		PROJECT::FindLogProp
//		NEST::FindLogProp

float LOG_PROP::Restrictivity(CStrings* attrnames)
{
	double new_ucard=1;

	// Derive the Minimal Independent Attribute Set (MIAS)
	// Note the algorithm here does not actually find the MIAS, 
	// for simply implementation. 
	// It only gives an independent attribute set.
	CStrings *AttrNames = attrnames ->Copy();
	FDS * fds = (FDS*) GetFDs()->Copy();
	for (uint i = 0; i<fds->size(); i++){
		if (AttrNames->Contains(fds->GetHeadNames(i))) 
			AttrNames->Remove(fds->GetTailName(i));
	}

	// Compute the unique cardinality for "attrs", 
	// assuming all the attributes in it are independent
    for (uint i=0; i < AttrNames->size(); i++) 
	{
		float attr_cucard = GetAttrProp(AttrNames->GetAt(i))->GetCuCard();
		if (attr_cucard != -1) new_ucard *= attr_cucard;
		else 
		{
			new_ucard = GetUCard();
			break;
		}
    }
    new_ucard = MIN(new_ucard, GetUCard());
	if (GetCard()) {
		float result = (float)(new_ucard/GetCard());
		assert(result>=0);
		return result;
	} else{
		assert(new_ucard>=0);
		return new_ucard;
	}
	assert(false);
	return 0;
};

ATTR_PROP * LOG_PROP::GetAttrProp(CString PathExp){
	for (uint i=0; i<PathExps->size(); i++)
		if (PathExp == PathExps->GetAt(i)) return AttrProps->GetAt(i);
		assert(false);
		return 0;
};

void LOG_PROP::AddAttrProp(CString PathExp, ATTR_PROP * ap){
	PathExps->push_back(PathExp);
	AttrProps->push_back(ap);
};



CString LOG_PROP::Dump() {
	CString os, temp;
	os.Format("%s%.0f%s%.0f%s%d","  Card:" , Card,
		"  UCard:" , UCard, "  PhyWidth", GetPhyWidth());
//	if (Keys->size()>0)
//	{
//		temp.Format("%s%s", "  Order:" , OrderToString(Order));
//		os += temp;
//	}
		
	if (Keys->size()>0)
	{
		os += "  Candidate Keys:";
                os += Keys->DumpNewLine();
	}
	
	if (FKeys && FKeys->size()>0)
	{
		os += "  Foreign Keys:";
                os += FKeys->DumpNewLine();
	}

	if (local) 
            os += ", Local";
        else
            os += ", Remote";

	return os;
}


// LOG_PROP dump function for COVE script
CString LOG_PROP::DumpCOVE() {
	CString os;
	os.Format("%d %d",(int)Card, (int)UCard);
        os += " { " + Attrs->Dump() + " }\n";
	return os;
}

}
