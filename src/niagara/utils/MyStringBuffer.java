package niagara.utils;

/**
 * Give StringBuffer a hash function. Hash so that string in string buffer will
 * hash to the same value as same string in class String.
 */

public class MyStringBuffer {
	private int length;
	private int capacity;
	private char[] buffer;
	private int hashCode;
	private boolean hcvalid;

	public MyStringBuffer() {
		length = 0;
		capacity = 0;
		ensureCapacity(16); // default initial capacity
		hcvalid = false;
	}

	public MyStringBuffer(int initial_capacity) {
		length = 0;
		capacity = 0;
		ensureCapacity(initial_capacity);
		hcvalid = false;
	}

	public MyStringBuffer append(String str) {
		int slen = str.length();
		ensureCapacity(length + slen);
		// args are srcBegin, srcEnd, dest array, destBegin
		// chars copied are index srcBegin -> srcEnd -1
		str.getChars(0, slen, buffer, length);
		length += slen;
		hcvalid = false;
		return this;
	}

	public MyStringBuffer append(MyStringBuffer sb) {
		ensureCapacity(length + sb.length);
		for (int i = 0; i < sb.length; i++)
			buffer[length + i] = sb.buffer[i];
		length += sb.length;
		hcvalid = false;
		return this;
	}

	public MyStringBuffer append(Object o) {
		append(String.valueOf(o));
		return this;
	}

	public int capacity() {
		return capacity;
	}

	public int length() {
		return length;
	}

	public void setLength(int newLen) {
		length = newLen;
	}

	public String toString() {
		// don't bother avoiding allocation, just do it now - I know
		// we're going to have to anyway
		return new String(buffer, 0, length);
	}

	public int hashCode() {
		if (hcvalid)
			return hashCode;

		// copied from java.lang.String
		// int hashCode = 0;
		hashCode = 0;
		for (int i = 0; i < length; i++) {
			hashCode = 31 * hashCode + buffer[i];
		}
		hcvalid = true;
		return hashCode;

		// hash code from java String class
		// s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1]
	}

	public boolean equals(Object obj) {
		MyStringBuffer sb = (MyStringBuffer) obj;
		if (length != sb.length)
			return false;

		for (int i = 0; i < length; i++) {
			if (buffer[i] != sb.buffer[i])
				return false;
		}
		return true;
	}

	private void ensureCapacity(int new_capacity) {
		if (new_capacity <= capacity)
			return;

		int old_capacity = capacity;
		capacity = new_capacity;
		char[] new_buffer = new char[capacity];
		for (int i = 0; i < old_capacity; i++) {
			new_buffer[i] = buffer[i];
		}
		buffer = new_buffer;
	}

}
