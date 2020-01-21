package join.helper;

import java.util.Iterator;

import join.datastructures.Block;
import join.datastructures.Tuple;
import join.manager.BlockManager;

/**
 * This iterator wraps a Block iterator and makes sure that the relevant blocks
 * are pinned and unpinned.
 * 
 */
public class PinningTupleIterator implements Iterator<Tuple>, AutoCloseable {

	private Block currentBlock = null;
	private Iterator<Tuple> current = null;
	private final Iterator<Block> iter;
	private final BlockManager blockManager;

	public PinningTupleIterator(Iterator<Block> iter, BlockManager blockManager) {
		this.iter = iter;
		this.blockManager = blockManager;
	}

	@Override
	public Tuple next() {
		if (current == null || !current.hasNext()) {
			if (currentBlock != null)
				blockManager.unpin(currentBlock);
			currentBlock = iter.next();
			blockManager.pin(currentBlock);
			current = currentBlock.iterator();
		}

		return current.next();
	}

	@Override
	public boolean hasNext() {
		return iter.hasNext() || (current != null && current.hasNext());
	}

	@Override
	public void close() {
		if (currentBlock != null)
			blockManager.unpin(currentBlock);
	}
}