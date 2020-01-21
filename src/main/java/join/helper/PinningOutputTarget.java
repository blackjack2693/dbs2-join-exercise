package join.helper;

import join.datastructures.Block;
import join.datastructures.Relation;
import join.datastructures.Tuple;
import join.manager.BlockManager;

/**
 * This helper class allows you to add tuples to a relation without manually
 * dealing with requesting new blocks, as well as pinning and unpinning of
 * blocks.
 * 
 */
public class PinningOutputTarget implements AutoCloseable {
	private Block currentBlock;
	private final Relation relation;
	private final BlockManager blockManager;
	private final boolean release;

	/**
	 * Constructs a new PinningOutputTarget that will unpin blocks, when they are no
	 * longer written to.
	 * 
	 * @param relation     The relation this class should write to.
	 * @param blockManager The block manager to use.
	 */
	public PinningOutputTarget(Relation relation, BlockManager blockManager) {
		this(relation, blockManager, true);
	}

	/**
	 * Constructs a new PinningOutputTarget that depending on the parameter
	 * <tt>release</tt>, will or will not unpin blocks, when they are no longer
	 * written to.
	 * 
	 * @param relation     The relation this class should write to.
	 * @param blockManager The block manager to use.
	 * @param release      Indicates, whether the blocks should be unpinned when
	 *                     they are no longer written to.
	 */
	public PinningOutputTarget(Relation relation, BlockManager blockManager, boolean release) {
		this.relation = relation;
		this.blockManager = blockManager;
		this.currentBlock = relation.getFreeBlock(blockManager);
		this.blockManager.pin(currentBlock);
		this.release = release;
	}

	public void addTuple(Tuple t) {
		if (!currentBlock.addTuple(t)) {
			if (release) {
				blockManager.unpin(currentBlock);
			}
			currentBlock = relation.getFreeBlock(blockManager);
			blockManager.pin(currentBlock);
			if (!currentBlock.addTuple(t)) {
				throw new IllegalStateException("block size too small");
			}
		}
	}

	@Override
	public void close() {
		if (release && currentBlock != null) {
			blockManager.unpin(currentBlock);
		}
	}
}