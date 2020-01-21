package join.manager;

import java.util.Map;
import java.util.WeakHashMap;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import join.datastructures.Block;

/**
 * Manages blocks, keeps track of pinned blocks and IO costs.
 */
public final class BlockManager {

	private static enum BlockState {
		FRESH, LOADED, UNLOADED, DIRTY;
	}

	private final int maxBlockNumber;
	private final int maxBlockSize;
	private long ioCount;

	private final Multiset<Block> pins;
	private final Map<Block, BlockGate> gates;

	/**
	 * Creates a new <tt>BlockManager</tt>, which can keep a maximum of
	 * <tt>maxBlockNumber</tt> of blocks with each a maximum size of
	 * <tt>maxBlockSize</tt> in memory.
	 * 
	 * @param maxBlockNumber maximum number of loaded blocks
	 * @param maxBlockSize   maximum size of a block
	 */
	public BlockManager(int maxBlockNumber, int maxBlockSize) {
		this.maxBlockNumber = maxBlockNumber;
		this.maxBlockSize = maxBlockSize;
		this.pins = HashMultiset.create();
		this.gates = new WeakHashMap<>();
		this.ioCount = 0;
	}

	/**
	 * Pins a block and loads it into memory, if necessary.
	 * 
	 * @param block the block to pin
	 * @throws IllegalStateException when the number of pinned blocks exceeds the
	 *                               maximum allowed number of pinned blocks
	 */
	public void pin(Block block) {
		if (!pins.contains(block) && gates.get(block).pin()) {
			ioCount++;
		}

		pins.add(block);
		if (pins.elementSet().size() > maxBlockNumber) {
			throw new IllegalStateException("cannot pin block, because maximum number of blocks is already pinned.");
		}
	}

	/**
	 * Unpins a block and writes it to disk, if necessary.
	 * 
	 * @param block the block to unpin
	 * @throws IllegalStateException when the block was not pinned before
	 */
	public void unpin(Block block) {
		if (!pins.remove(block)) {
			throw new IllegalStateException("cannot unpin block that was not pinned before.");
		}
		if (!pins.contains(block) && gates.get(block).unpin()) {
			ioCount++;
		}
	}

	/**
	 * Creates a new block in fresh-state
	 * 
	 * @param inMemory wheter or not the block should be in-memory
	 * @return the new block
	 */
	public Block getFreeBlock(boolean inMemory) {
		BlockGate gate = new BlockGate(true, inMemory);
		Block b = new Block(maxBlockSize, gate);
		gates.put(b, gate);
		return b;
	}

	/**
	 * Returns the number of blocks that can still be pinned, before the memory
	 * limit is reached
	 * 
	 * @return the number of blocks that can still be pinned, before the memory
	 *         limit is reached
	 */
	public int getFreeBlockCount() {
		return maxBlockNumber - pins.elementSet().size();
	}

	/**
	 * Returns the number of IO operations since the creation of the block manager
	 * 
	 * @return the number of IO operations since the creation of the block manager
	 */
	public long getIOCount() {
		return ioCount;
	}

	public void outputStats() {
		System.out.println("Number of pins: " + pins.elementSet().size());
		System.out.println("Free block count: " + getFreeBlockCount());
	}

	public final static class BlockGate {

		private BlockState state;
		private boolean inMemory;

		private BlockGate(boolean fresh, boolean inMemory) {
			this.state = fresh ? BlockState.FRESH : BlockState.UNLOADED;
			this.inMemory = inMemory;
		}

		private boolean pin() {
			if (inMemory && state != BlockState.FRESH)
				throw new IllegalStateException("cannot pin inmemory block again");

			boolean load = state == BlockState.UNLOADED;
			this.state = BlockState.LOADED;
			return load;
		}

		private boolean unpin() {
			boolean write = state == BlockState.DIRTY;
			this.state = BlockState.UNLOADED;
			return !inMemory && write;
		}

		public boolean canAccess() {
			return state == BlockState.LOADED || state == BlockState.DIRTY;
		}

		public void markDirty() {
			state = BlockState.DIRTY;
		}

	}

}
