package join.datastructures;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.Iterators;

import join.manager.BlockManager;

/**
 * A relation, which is mainly a list of blocks (or rather block references).
 * 
 * A relation can be in-memory, in which case its block are only maintained as
 * long they are pinned.
 */
public final class Relation implements Iterable<Block> {
	private final List<Block> blocks;
	private final boolean inMemory;

	/**
	 * Constructs a new relation that will not be in-memory.
	 */
	public Relation() {
		this(false);
	}

	/**
	 * Constructs a new relation.
	 * 
	 * @param inMemory Whether this relation should be in-memory.
	 */
	public Relation(boolean inMemory) {
		this.blocks = new ArrayList<>();
		this.inMemory = inMemory;
	}

	@Override
	public Iterator<Block> iterator() {
		return Iterators.unmodifiableIterator(blocks.iterator());
	}

	public Block getFreeBlock(BlockManager blockManager) {
		Objects.requireNonNull(blockManager, "block manager must not be null");

		Block b = blockManager.getFreeBlock(inMemory);
		blocks.add(b);
		return b;
	}

	public int getBlockCount() {
		return blocks.size();
	}
}
