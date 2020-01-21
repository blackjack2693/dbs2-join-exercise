package join;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import join.algorithms.HashEquiJoin;
import join.algorithms.Join;
import join.algorithms.NestedLoopEquiJoin;
import join.datastructures.Relation;
import join.datastructures.Tuple;
import join.helper.PinningOutputTarget;
import join.manager.BlockManager;

public class Main {

	@Parameter(names = "-blockCount", description = "Maximum number of pinned blocks")
	private Integer blockCount = 50;
	@Parameter(names = "-blockSize", description = "Maximum block size")
	private Integer blockSize = 100000;

	@Parameter(names = "-bucketCount", description = "Bucket count for hash algorithms")
	private Integer bucketCount = 5;

	@Parameter(names = { "-r1", "-relation1" }, required = true, description = "csv file 1 to join")
	private String relationPath1;
	@Parameter(names = { "-r2", "-relation2" }, required = true, description = "csv file 2 to join")
	private String relationPath2;

	@Parameter(names = { "-j1", "-join1" }, required = true, description = "join attribute index 1")
	private Integer joinAttribute1;
	@Parameter(names = { "-j2", "-join2" }, required = true, description = "join attribute index 2")
	private Integer joinAttribute2;

	@Parameter(names = { "-s1", "-scale1" }, description = "scale factor 1")
	private Integer scaleFactor1 = 1;
	@Parameter(names = { "-s2", "-scale2" }, description = "scale factor 2")
	private Integer scaleFactor2 = 1;

	public static void main(String[] args) throws IOException {
		Main main = new Main();
		JCommander.newBuilder().addObject(main).build().parse(args);
		main.run();
	}

	private void run() throws IOException {
		BlockManager blockManager = new BlockManager(blockCount, blockSize);

		Relation relation1 = loadRelationFromCSV(blockManager, Paths.get(relationPath1), scaleFactor1);
		Relation relation2 = loadRelationFromCSV(blockManager, Paths.get(relationPath2), scaleFactor2);
		System.out.println(
				"Input relation sizes (blocks): " + relation1.getBlockCount() + " " + relation2.getBlockCount());

		Join nlj = new NestedLoopEquiJoin(blockManager);
		Multiset<Tuple> resultNLJ = getJoinResult(relation1, relation2, nlj);
		System.out.println("NLJ result: " + resultNLJ.size());

		List<Join> joinsToEvaluate = new ArrayList<>();
		joinsToEvaluate.add(new HashEquiJoin(bucketCount, blockManager));

		System.out.println();

		for (Join algorithm : joinsToEvaluate) {
			System.out.println(algorithm.getClass().getCanonicalName());

			System.out.println("IO cost estimate: " + algorithm.getIOEstimate(relation1, relation2));

			long prevIOCount = blockManager.getIOCount();
			Multiset<Tuple> joinResult = getJoinResult(relation1, relation2, algorithm);
			System.out.println("Result size: " + joinResult.size());
			System.out.println("Result equals NLJ: " + joinResult.equals(resultNLJ));
			System.out.println("Real IO cost:" + (blockManager.getIOCount() - prevIOCount));
			System.out.println();
		}
	}

	protected Multiset<Tuple> getJoinResult(Relation relation1, Relation relation2, Join nlj) {
		Multiset<Tuple> result = HashMultiset.create();
		nlj.join(relation1, joinAttribute1, relation2, joinAttribute2, result::add);
		return result;
	}

	private static Relation loadRelationFromCSV(BlockManager blockManager, Path p, int scaleFactor) throws IOException {
		Relation relation = new Relation();
		CSVParser parser = CSVFormat.TDF.parse(Files.newBufferedReader(p));
		try (PinningOutputTarget target = new PinningOutputTarget(relation, blockManager)) {
			for (CSVRecord r : parser) {
				int size = r.size();
				String[] data = new String[size];
				for (int i = 0; i < data.length; ++i) {
					data[i] = r.get(i);
				}
				for (int i = 0; i < scaleFactor; ++i) {
					target.addTuple(new Tuple(data));
				}

			}
		}
		return relation;
	}
}
