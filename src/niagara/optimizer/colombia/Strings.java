package niagara.optimizer.colombia;

import java.util.ArrayList;

//
//	A set of attribute names
//
class Strings {
    private ArrayList attrnames;

    public Strings() {
	attrnames = new ArrayList();
    }

    public Strings(Strings other) {
	attrnames.addAll(other.attrnames);
    }
    public boolean isEmpty() { return attrnames.isEmpty(); }
    public boolean notEmpty() { return !attrnames.isEmpty(); }

    // Check whether attrname is in this set
    public boolean contains(String attrname) {
	for (int i = 0; i < size(); i++) {
	    // XXX vpapad: changing == to equals options: equals,
	    // ==, keep an extra hashtable, intern strings, ...
	    if (attrnames.get(i).equals(attrname)) 
		return true;
	}
	return false;
    }

	// Check whether attrnames is subset of this set
    public boolean contains(Strings other) {
	if (size() < other->size()) return false;
	for (uint i=0; i<other->size(); i++)
	    {
		if (!(Contains(other->GetAt(i)))) return false;
	    };
	return true;
    }

	// Check whether there is overlap between this and other
	bool Overlap(Strings * other)
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
	bool operator==(Strings &other)
	{
		if (size() != other.size()) return false;
		for (uint i=0; i<size(); i++)
		{
			if (!(Contains(other.GetAt(i)))) return false;
		};
		return true;
	};
	
    //remove the attributes that are not in the "attrs" list. 
    Strings projection(Strings attrList) {
	Strings attrs = new Strings;
	for (int i=size()-1;i>=0; i--) {
	    if (AttrNames->Contains(GetAt(i))) 
		attrs->push_back(GetAt(i));
	}
	return attrs;
    }

// 	// To get around a bug in CArray<CString>: 
// 	//		GetAt return "" for any index greater than 0;
// 	CString & GetAt(int i) { return this->at(i);};

    Strings copy() {
	return new Strings(this);
    }
    
    // XXX vpapad: why is this here?!?
//     void attachPathName(CString varname) {
// 	for (uint i=0; i<size(); i++) {
// 	    assert((*this)[i] != "");
// 	    (*this)[i] =  varname + "." + (*this)[i];
// 	}
//     }
    
    int findIdx(String s) {
	return attrnames.lastIndexOf(s);
    }
    
    // XXX vpapad: commented out from the C++ version
    // Remove duplicates
    //bool Add(CString c)
    //{
    //	if (Contains(c)) return false;
    //	InsertAt(size(), c);
    //	return true;
    //};
    
    void addAll(Strings other) {
	for (int i = 0; i < other.size(); i++)
	    if (!(contains(other.get(i))))
		attrnames.add(other.get(i));
    }

    // Same as AddAll unless duplicates are not removed
    void concat(Strings other) {
	attrnames.addAll(other.attrnames);
    }
    
    // Remove an element 
    public boolean remove(String str) {
	int index = attrnames.lastIndexOf(str);
	if (index >= 0) {
	    attrnames.remove(index);
	    return true;
	}
	
	return false;
    }	

    void distinct() {
	// XXX vpapad: Horribly slow
	ArrayList temp = new ArrayList(attrnames.size());

	for (int i = 0; i < size(); i++) {
	    if (!temp.contains(attrnames.get(i))) 
		temp.add(attrnames.get(i));
	}
	attrnames.clear();
	attrnames.addAll(temp);
    }
    
    String dump(string delimiter) {
	if (size() == 0) 
	    return "";

	StringBuffer sb = new StringBuffer();
	sb.append(attrnames.get(0));
	for (int i = 1; i < size(); i++) {
	    sb.append(delimiter);
	    sb.append(attrnames.get(i));
	}
	return sb.toString();
    }

    String dump() {
	return dump(", ");
    }
        
    String dumpNewLine() {
	return dump("\n");
    }
}
