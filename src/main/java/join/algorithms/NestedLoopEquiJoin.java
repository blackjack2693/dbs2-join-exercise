package join.algorithms;

import java.util.function.Consumer;

import join.datastructures.Block;
import join.datastructures.Relation;
import join.datastructures.Tuple;
import join.manager.BlockManager;

public class NestedLoopEquiJoin implements Join {

	protected final BlockManager blockManager;

	public NestedLoopEquiJoin(BlockManager blockManager) {
		this.blockManager = blockManager;
	}

	@Override
	public void join(Relation relation1, int joinAttribute1, Relation relation2, int joinAttribute2,
			Consumer<Tuple> consumer) {

		// use smaller relation as outer relation
		boolean swapped = relation2.getBlockCount() < relation1.getBlockCount();
		Relation outer = swapped ? relation2 : relation1;
		Relation inner = swapped ? relation1 : relation2;

		for (Block leftBlock : outer) {
			blockManager.pin(leftBlock);
			for (Block rightBlock : inner) {
				blockManager.pin(rightBlock);
				Join.joinTuples(swapped ? rightBlock : leftBlock, joinAttribute1, swapped ? leftBlock : rightBlock,
						joinAttribute2, consumer);
				blockManager.unpin(rightBlock);
			}
			blockManager.unpin(leftBlock);
		}
	}

	@Override
	public int getIOEstimate(Relation relation1, Relation relation2) {
		boolean swapped = relation2.getBlockCount() < relation1.getBlockCount();
		Relation outer = swapped ? relation2 : relation1;
		Relation inner = swapped ? relation1 : relation2;

		return outer.getBlockCount() * (1 + inner.getBlockCount());
	}

}
