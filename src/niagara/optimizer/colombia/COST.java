package niagara.optimizer.colombia;

/**
Cost of executing a physical operator, expression or multiexpression
Cost cannot be associated with a multiexpression, since cost is
determined by properties desired.  For example, a SELECT will cost
more if sorted is required.

Cost value of -1 = infinite cost.  
Any other negative cost is considered an error.
*/
public class Cost {
    // XXX vpapad: -1 is a poor choice for infinite cost
    // We could use Double.POSITIVE_INFINITY and eliminate
    // lots of useless checks

    private double value; // Later this may be a base class specialized
    // to various costs: CPU, IO, etc.
    
    public Cost() {
        value = 0;
    }

    public Cost(double value) {
        assert value == -1 || value >= 0;
        this.value = value;
    }

    public Cost(Cost c) {
        this.value = c.value;
    }
    //finalCost() makes "this" equal to the total of local and input costs. 
    // It is an error if any input is null.  
    // In a parallel environment, this may involve max.
    void finalCost(Cost localCost, Cost[] totalInputCost) {
        value = localCost.value;
        if (totalInputCost == null) return;
        for (int i = 0; i < totalInputCost.length; i++) {
            assert(totalInputCost[i] != null);
            value += totalInputCost[i].value;
        }
    }

    double getValue() {
        return value;
    }

    public String toString() {
        return Double.toString(value);
    }

    public boolean equals(Object other) {
        if (other == null || !(other instanceof Cost)) return false;
        if (other.getClass() != Cost.class) return other.equals(this);
        return (value == ((Cost) other).value);
    }
    
    public boolean greaterThanEqual(Cost other) {
        if (value == -1)
            return true;
        if (other.value == -1) // -1 means Infinite 
            return false;
        return this.value >= other.value;
    }

    public boolean nonZero() {
        return value > 0 || value == -1;
    }

    public boolean lessThan(Cost other) {
        if (value == -1)
            return (false);

        if (other.value == -1) // -1 means Infinite 
            return (true);

        return (this.value < other.value);
    }

    public boolean greaterThan(Cost other) {
        if (value == -1)
            return (true);

        if (other.value == -1) // -1 means Infinite 
            return (false);

        return (this.value > other.value);
    }

    public void subtract(Cost other) {
       if (value == -1 || other.value == -1)    // -1 means Infinite 
            value = -1; 
        else
            value -= other.value;
    }

    public void add(Cost other) {
       if (value == -1 || other.value == -1)    // -1 means Infinite 
            return;
        else
            value += other.value;
    }
    
    public Cost times(double factor) {
        return new Cost(value*factor);
    }
    
    public void divide(int parts) {
        if (value == -1)
            return;
        value /= parts;
    }
    
    /* XXX vpapad: commenting out overriden operators
       to see which of these are needed
    
    
     Cost& operator*=(double EPS) 
    {
    	assert(EPS > 0) ;
    
    	if(Value==-1)	// -1 means Infinite 
    		Value = 0;	
    	else
    		Value *= EPS;
    
        return (*this);
    }
    
     Cost& operator/=(int arity) 
    {
    	assert(arity > 0) ;
    
    	if(Value==-1)	// -1 means Infinite 
    		Value = 0;	
    	else
    		Value /= arity;
    
        return (*this);
    }
    
     Cost& operator=(const Cost &other) 
    {
    	this->Value = other.Value;
        return (*this);
    }
    
    
     Cost& operator*(double EPS) 
    {
    	assert(EPS >= 0) ;
    
    	Cost *temp ;
    	if(Value==-1)	// -1 means Infinite 
    		temp = new Cost (0);	
    	else
    		temp = new Cost (Value * EPS);
    	return (*temp);
    	
    }
    
     Cost& operator/(int arity) 
    {
    	assert(arity > 0) ;
    
    	Cost *temp;
    	if(Value==-1)	// -1 means Infinite 
    		temp = new Cost(0);	
    	else
    		temp = new Cost (Value / arity);
    
        return (*temp);
    }
    */
}
