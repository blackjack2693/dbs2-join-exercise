package join.algorithms;

import java.util.Objects;
import java.util.function.Consumer;

import join.datastructures.Relation;
import join.datastructures.Tuple;

public interface Join {

	void join(Relation relation1, int joinAttribute1, Relation relation2, int joinAttribute2, Consumer<Tuple> consumer);

	int getIOEstimate(Relation relation1, Relation relation2);

	static void joinTuples(Iterable<Tuple> leftBlock, int joinAttribute1, Iterable<Tuple> rightBlock,
			int joinAttribute2, Consumer<Tuple> consumer) {
		for (Tuple t1 : leftBlock) {
			for (Tuple t2 : rightBlock) {
				// join-condition satisfied?
				if (Objects.equals(t1.getData(joinAttribute1), t2.getData(joinAttribute2))) {
					consumer.accept(Join.joinTuple(t1, joinAttribute1, t2, joinAttribute2));
				}
			}
		}
	}

	static Tuple joinTuple(Tuple t1, int joinAttribute1, Tuple t2, int joinAttribute2) {
		String[] data = new String[t1.getAttributeCount() + t2.getAttributeCount()];

		int pos = 0;
		for (int i = 0; i < t1.getAttributeCount(); ++i)
			data[pos++] = t1.getData(i);
		for (int i = 0; i < t2.getAttributeCount(); ++i)
			data[pos++] = t2.getData(i);
		return new Tuple(data);
	}
}
