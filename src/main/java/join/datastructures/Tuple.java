package join.datastructures;

import java.util.Arrays;
import java.util.Objects;

/**
 * A very simple tuple that can only contain Strings, indexed by position.
 */
public final class Tuple {

	private static final int CONSTANT_COST = 4;

	private final String[] data;

	public Tuple(String[] data) {
		Objects.requireNonNull(data, "data must not be null");

		this.data = Arrays.copyOf(data, data.length);
	}

	public String getData(int pos) {
		return data[pos];
	}

	public int getAttributeCount() {
		return data.length;
	}

	public int getSizeInBytes() {
		int size = CONSTANT_COST;
		for (String s : data) {
			size += s.length() + CONSTANT_COST;
		}
		return size;
	}

	@Override
	public String toString() {
		return "Tuple [data=" + Arrays.toString(data) + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(data);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Tuple other = (Tuple) obj;
		if (!Arrays.equals(data, other.data))
			return false;
		return true;
	}

}
