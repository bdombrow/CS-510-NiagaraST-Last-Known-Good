package niagara.utils;

import java.util.ArrayList;


/**
 * <code>Guard</code> encapsulates fp objects.
 * @author rfernand
 * @version 1.0
 *
 */
public class Guard {
	// Fields
	private ArrayList<FeedbackPunctuation> _guards;

	// Properties
	public ArrayList<FeedbackPunctuation> elements() {
		return _guards;
	}

	// Ctor
	public Guard() {
		_guards = new ArrayList<FeedbackPunctuation>();
	}
	
	// Methods
	public void add(FeedbackPunctuation fp) {
		if(!_guards.contains(fp)) {
			_guards.add(fp);
		}
	}
	
	public void remove(FeedbackPunctuation fp){
		int index = _guards.indexOf(fp);
		if(index > -1) {
			_guards.remove(index);
		}
	}

}
