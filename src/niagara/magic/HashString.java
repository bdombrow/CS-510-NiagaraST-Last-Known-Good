package niagara.magic;

public class HashString {

	public String s;
	private int hashCode;

	public HashString(String s) {
		this.s = s;
		hashCode = s.hashCode();
	}

	public int hashCode() {
		return hashCode;
	}
}
