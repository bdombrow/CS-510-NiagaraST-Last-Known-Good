package niagara.optimizer.colombia;

//
//	A set of attribute names
//
class CStrings: public std::vector<CString> {
public:

	CStrings(){};

	virtual ~CStrings() {}; // XXX Vassilis let's see...

	CStrings(CStrings & attrnames){
		for (uint i=0; i<attrnames.size(); i++){
			push_back(attrnames.GetAt(i));
		}
	};
	bool isEmpty(){return empty();}
	bool notEmpty(){return !empty();}
	// Check whether attrname is in this set
	bool Contains(CString attrname)
	{
		for (uint i=0; i<size(); i++)
		{
			if (GetAt(i)==attrname) return true;
		};
		return false;
	};

	// Check whether attrnames is subset of this set
	bool Contains(CStrings * other)
	{
		if (size() < other->size()) return false;
		for (uint i=0; i<other->size(); i++)
		{
			if (!(Contains(other->GetAt(i)))) return false;
		};
		return true;
	};

	// Check whether there is overlap between this and other
	bool Overlap(CStrings * other)
	{
		for (uint i=0; i<other->size(); i++)
		{
			if (Contains(other->GetAt(i))) return true;
		};
		for (uint i=0; i<size(); i++)
		{
			if (other->Contains(GetAt(i))) return true;
		};
		return false;
	};
	// Check if they are equivalet SETS
	bool operator==(CStrings &other)
	{
		if (size() != other.size()) return false;
		for (uint i=0; i<size(); i++)
		{
			if (!(Contains(other.GetAt(i)))) return false;
		};
		return true;
	};
	
	//remove the attributes that are not in the "attrs" list. 
	CStrings * Projection(CStrings * AttrNames)
	{
		CStrings * attrs = new CStrings;
		for (int i=size()-1;i>=0; i--) {
			if (AttrNames->Contains(GetAt(i))) 
				attrs->push_back(GetAt(i));
		}
		return attrs;
	};

	// To get around a bug in CArray<CString>: 
	//		GetAt return "" for any index greater than 0;
	CString & GetAt(int i) { return this->at(i);};

	CStrings * Copy(){
		return new CStrings(*this);
	};
	void AttachPathName(CString varname){
		for (uint i=0; i<size(); i++) {
			assert((*this)[i] != "");
			(*this)[i] =  varname + "." + (*this)[i];
		};
	};
	
	int FindIdx(CString c){
		for (uint i=0; i<size(); i++) {
			if(c==GetAt(i)) return i;
		};
		return -1;
	};
	// Remove duplicates
	//bool Add(CString c)
	//{
	//	if (Contains(c)) return false;
	//	InsertAt(size(), c);
	//	return true;
	//};

	void AddAll(CStrings & other)
	{
		for (uint i=0; i<other.size(); i++)
			if (!(Contains(other[i]))) 
				push_back(other[i]);
	};

	// Same as AddAll unless duplicates are not removed
	void Concat(CStrings & other)
	{
		for (uint i=0; i<other.size(); i++)
				push_back(other[i]);
	};

	// Remove an element 
	bool Remove(CString str)
	{
            uint i;
		for (i=0; i< size()-1; i++)
			if (GetAt(i) == str) break;
                if(i == size()) return false;		
                this->erase(begin()+i);  
                return true;
	}	

	void Distinct()
	{
		CStrings * temp = new CStrings;
		for (uint i=0; i< size(); i++)
		{
			if (!temp->Contains(GetAt(i))) 
                            temp->push_back(GetAt(i));
		};
		clear();
		copy(temp->begin(), temp->end(), begin());
	};

	CString Dump(const char* delimiter) {
		CString os;
		if (size()==0) return "";
		os = GetAt(0);
		for (uint i=1; i<size(); i++)
			os += delimiter + GetAt(i);
		return os;
	};

        CString Dump() {
            return Dump(", ");
        }
        
        CString DumpNewLine() {
            return Dump("\n");
        }

	CString DumpJava() {
		if (size()==0) return "{}";
		CString os = "{\"" + GetAt(0) + "\"";
		for (uint i=1; i<size(); i++)
			os += ", \"" +GetAt(i)+"\"";
		os +="}";
		return os;
	};
};
