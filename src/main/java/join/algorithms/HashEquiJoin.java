package join.algorithms;

import java.util.function.Consumer;

import join.datastructures.Block;
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
			Consumer<Tuple> consumer) 
	{
		// Put tuples of each relation into buckets
		Relation[] relation1HashTable= setupHashTable(relation1, joinAttribute1,false);
		Relation[] relation2HashTable = setupHashTable(relation2, joinAttribute2, true);

		NestedLoopEquiJoin nestedLoopJoin = new NestedLoopEquiJoin(blockManager);
		for (int i = 0; i < numBuckets; ++i)
		{
			// Join each relation which represents one bucket
			nestedLoopJoin.join(relation1HashTable[i], joinAttribute1, relation2HashTable[i], joinAttribute2, consumer);
		}
	}

	private int getHashValue(Tuple tuple, int joinAttribute, int size)
	{
		return Math.abs(tuple.getData(joinAttribute).hashCode()) % numBuckets;
	}

	private Relation[] setupHashTable(Relation relation, int joinAttribute, boolean keepPinned){
		// We have a relation foe every hashValue.
		Relation[] relationHashTable = new Relation[numBuckets];
		
		// Each HashValue buffers maximum one block to add tuples
		Block[] hashTableForBlocks = new Block[numBuckets];

		for(Block block: relation)
		{
			blockManager.pin(block);
			for(Tuple tuple: block)
			{
				int hashValue = getHashValue(tuple, joinAttribute, numBuckets);
				if(relationHashTable[hashValue] == null)
				{
					// Because no relation exists for this HashValue, we create one
					relationHashTable[hashValue] = new Relation(false);
				}
				
				Relation bucketRelation = relationHashTable[hashValue];
				if(hashTableForBlocks[hashValue] == null)
				{
					// Because we have no block buffered for this HashValue, we buffer a new Block from the relation 
					Block newBucketBlock = bucketRelation.getFreeBlock(blockManager);
					blockManager.pin(newBucketBlock);
					hashTableForBlocks[hashValue] = newBucketBlock;
				}
				Block bucketBlock = hashTableForBlocks[hashValue];
				if(!bucketBlock.addTuple(tuple))
				{
					// Buffered block for this HasValue is full: we get a new one 
					blockManager.unpin(bucketBlock);
					Block newBucketBlock = bucketRelation.getFreeBlock(blockManager);
					blockManager.pin(newBucketBlock);
					hashTableForBlocks[hashValue] = newBucketBlock;
					//Add current tuple to Hashtable
					newBucketBlock.addTuple(tuple);
				}
			}
			blockManager.unpin(block);
		}
		//unpin all blocks. 
		for(Block block: hashTableForBlocks)
		{
			if (block == null || keepPinned)
				continue;
			blockManager.unpin(block);
		}

		return relationHashTable;
	}

	@Override
	public int getIOEstimate(Relation relation1, Relation relation2) {
		return 3 * (relation1.getBlockCount() + relation2.getBlockCount());
	}
}
