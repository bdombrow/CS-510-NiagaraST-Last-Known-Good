package niagara.logical;

/**
 * This class is used to represent a similarity specification.
 * 
 */

public class SimilaritySpec {

	public enum SimilarityType {
		AllDays, WeekDays, SameDayOfWeek
	};

	private SimilarityType sType;
	private int numOfDays;
	private int numOfMins;

	// -1 means weather is not part of our similarity metric;
	private boolean weather;

	public SimilaritySpec(String type, int _numOfDays, int _numOfMins,
			boolean weather) {
		// do something smart
		if ((type.compareToIgnoreCase("all") == 0)
				|| (type.compareToIgnoreCase("AllDays") == 0))
			sType = SimilarityType.AllDays;
		else if ((type.compareToIgnoreCase("week") == 0)
				|| (type.compareToIgnoreCase("WeekDays") == 0))
			sType = SimilarityType.WeekDays;
		else if ((type.compareToIgnoreCase("sdow") == 0)
				|| (type.compareToIgnoreCase("SameDayOfWeek") == 0))
			sType = SimilarityType.SameDayOfWeek;
		else
			System.err.println("unsupported similarity type - " + type);

		numOfDays = _numOfDays;
		numOfMins = _numOfMins;
		weather = false;
	}

	public void setWeather(boolean weather) {
		this.weather = weather;
	}

	public SimilarityType getSimilarityType() {
		return sType;
	}

	public int getNumOfDays() {
		return numOfDays;
	}

	public int getNumOfMins() {
		return numOfMins;
	}

	public boolean getWeather() {
		return weather;
	}

	public String toString() {
		switch (sType) {
		case AllDays:
			return "All days";
		case WeekDays:
			return "Week days";
		case SameDayOfWeek:
			return "Same day of week";
		default:
			assert false : "Invalid similarity type";
			return null;
		}
	}

	public int hashCode() {
		return sType.hashCode();
	}

}
