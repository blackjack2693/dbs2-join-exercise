package join.datastructures;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.Iterators;

import join.manager.BlockManager.BlockGate;

/**
 * A block that contains tuples. Consider instances of this class as references
 * to a block. It depends on the pin state of this block whether its tuples can
 * be accessed or not. Pinning of blocks is realized by the BlockManager.
 */
public final class Block implements Iterable<Tuple> {

	private final int maxSize;
	private final List<Tuple> tuples;

	private int currentSize;

	private final BlockGate gate;

	public Block(int maxSize, BlockGate gate) {
		Objects.requireNonNull(gate, "block gate must not be null");

		this.maxSize = maxSize;
		this.currentSize = 0;
		this.tuples = new ArrayList<>();
		this.gate = gate;
	}

	public boolean addTuple(Tuple tuple) {
		Objects.requireNonNull(gate, "tuple must not be null");

		checkAccess("cannot write to unpinned block");

		int tupleSize = tuple.getSizeInBytes();
		if (currentSize + tupleSize <= maxSize) {
			gate.markDirty();
			this.tuples.add(tuple);
			this.currentSize += tupleSize;
			return true;
		}
		return false;
	}

	@Override
	public Iterator<Tuple> iterator() {
		return new GatedIterator<>(Iterators.unmodifiableIterator(tuples.iterator()));
	}

	public Iterator<Tuple> sortedIterator(int attribute) {
		List<Tuple> sorted = new ArrayList<>(tuples);
		sorted.sort(Comparator.comparing(t -> t.getData(attribute)));
		return new GatedIterator<>(Iterators.unmodifiableIterator(sorted.iterator()));
	}

	private void checkAccess(String error) {
		if (!gate.canAccess())
			throw new IllegalStateException(error);
	}

	/**
	 * This iterator wraps another iterator and makes sure that the associated
	 * BlockGate allows access to the underlying iterator.
	 */
	private final class GatedIterator<T> implements Iterator<T> {

		private static final String READ_ERROR = "cannot read from unpinned block";
		private final Iterator<T> iter;

		public GatedIterator(Iterator<T> iter) {
			Objects.requireNonNull(iter, "iterator must not be null");

			this.iter = iter;
		}

		@Override
		public boolean hasNext() {
			checkAccess(READ_ERROR);
			return iter.hasNext();
		}

		@Override
		public T next() {
			checkAccess(READ_ERROR);
			return iter.next();
		}
	}

}
