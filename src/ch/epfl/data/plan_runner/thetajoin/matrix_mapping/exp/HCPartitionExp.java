package ch.epfl.data.plan_runner.thetajoin.matrix_mapping.exp;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import ch.epfl.data.plan_runner.thetajoin.matrix_mapping.ArrangementIterator;
import ch.epfl.data.plan_runner.thetajoin.matrix_mapping.CubeNAssignmentBruteForce;

public class HCPartitionExp {

	public static void main(String[] args) throws IOException {

		// Bootstrap squall
		long[] sizes = { 4, 2 };
		new CubeNAssignmentBruteForce(sizes, 10, -1);

//		System.setOut(new PrintStream(new FileOutputStream("exp_partition.csv", false)));
		System.out.format("#dims\t#joiners\trelations\t#regions\tassignment\truntime(ms)%n");
		for (int d = 8; d <= 8; d++) {
			for (int r = 1000; r <= 1000; r+=100) {
				expInstance(d, r);
			}
		}

	}

	private static void expInstance(int d, int r) {
		ArrangementIterator me = new ArrangementIterator(Arrays.asList(100, 1000, 10000), d, true);

		while (me.hasNext()) {
			List<Integer> combination = me.next();
			// long[] sizes = ArrayUtils.toPrimitive(combination.toArray(new
			// Long[0]));
			long[] sizes = new long[combination.size()];
			for (int i = 0; i < sizes.length; i++) {
				sizes[i] = combination.get(i);
			}

			boolean timeout = !instance(d, r, sizes);
			if (timeout) break;
		}
	}

	private static boolean instance(int d, final int r, final long[] sizes) {
		MyTask task = new MyTask(sizes, r);
		try {
			TimeoutController.execute(task, 30000L);
		} catch (TimeoutException e) {
//			e.printStackTrace();
		}
		CubeNAssignmentBruteForce result = task.result;
		if (result != null) {
			long totalTime = task.totalTime;
			System.out.format("%d\t%d\t%s\t%d\t%s\t%s%n", d, r, Arrays.toString(sizes), result.getNumberOfRegions(), result.toString(), totalTime);
			return true;
		} else {
			System.out.format("%d\t%d\t%s\t%d\t%s\t%s%n", d, r, Arrays.toString(sizes), 0, "NA", ">30s");
			return false;
		}
	}

}

class MyTask implements Runnable {
	CubeNAssignmentBruteForce result = null;
	long[] sizes;
	int r;
	long totalTime;

	public MyTask(long[] sizes, int r) {
		this.sizes = sizes;
		this.r = r;
	}

	@Override
	public void run() {
		long startTime = System.currentTimeMillis();
		result = new CubeNAssignmentBruteForce(sizes, r, -1);
		totalTime = System.currentTimeMillis() - startTime;
	}

}
