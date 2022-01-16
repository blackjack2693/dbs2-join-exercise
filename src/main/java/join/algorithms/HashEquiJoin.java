package join.algorithms;

import java.util.function.Consumer;

import join.datastructures.Relation;
import join.datastructures.Tuple;
import join.manager.BlockManager;

public class HashEquiJoin implements Join {
	protected final int numBuckets;
	protected final BlockManager blockManager;

	public HashEquiJoin(int numBuckets, BlockManager blockManager) {
		this.numBuckets = numBuckets;
		this.blockManager = blockManager;
	}

	@Override
	public void join(Relation relation1, int joinAttribute1, Relation relation2, int joinAttribute2,
			Consumer<Tuple> consumer) {
		// TODO: hash
		// M = freeBlocks
		int freeBlocks = blockManager.getFreeBlockCount();

		Block [] buffers = new Block[freeBlocks - 1];

		for (int i = 0; i < freeBlocks - 1; i++) {
			buffers[i] = blockManager.getFreeBlock();
		}
		//get Tuple
		for(int b = 0; b < relation1.getBlockCount(); b++) {
			Block block = relation1.getBlock(b);
			blockManager.pin(block);
			while(block.hasNext()) {
				Tuple tuple = block.Next()
				int hash = tuple.hashCode();
				if(!buffers[hash].addTuple(tuple)) {
					buffers[hash]
					//write to disk and safe the hash to grab later

					blockmanager.unpin(buffers[hash]);
					buffers[hash] = blockManager.getFreeBlock();
					buffers[hash].addTuple(tuple)
				}
		
				}
		}
		for(int b = 0; b < buffers.length; b++) {
			blockManager.unpin(buffers[b]);
		}
		NestedLoopEquiJoin nestedLoopJoin = new NestedLoopEquiJoin(blockManager);
		for (int i = 0; i < numBuckets; ++i) {
			// TODO: join
		}
	}

	@Override
	public int getIOEstimate(Relation relation1, Relation relation2) {
		return 3 * (relation1.getBlockCount() + relation2.getBlockCount());
	}

}
