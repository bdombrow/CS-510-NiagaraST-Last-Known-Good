package niagara.firehose;

import java.util.Random;

/***
 * class to generate IDs for the auction generator is capable of generating new
 * ids or generating existing IDs distributions supported are exponential and
 * uniform
 * 
 * Maintains a Zipfian distribution and can return a random integer drawn from
 * the distribution. Zipfian distribution is freq(i) ~ 1/i^exp, where exp close
 * to 1 Strictly sum(i=1 to N) Pi = numItems * avgNumEventsPerItem Pi is
 * frequency and formally Pi = sum(i=1 to N) 1/i^ex C =
 * (numItems*avgNumEvents)/(sum(i=i to N) 1/i^exp)
 */
class ZipfDistr {

	private double exp; // exponenent of zipf distribution
	private int numItems;
	private double C; // normalizing constant for distribution
	private Random rnd = new Random(18384);
	private double probIN[];
//	private String name;
	private double normalizer;

	public ZipfDistr(double exponent, int numItems, String name) {
		this.exp = exponent;
		this.numItems = numItems;
//		this.name = name;
		probIN = new double[numItems];
		calculateConstAndProbs();
	}

	// return a random integer drawn from the given zipf distribution
	// use rejectoin method
	public int nextInt() {
		// choose random 0 to numItems
		// accept or reject this item num
		boolean found = false;
		int itemNum = 0;
		while (!found) {
			// get possible item num
			itemNum = rnd.nextInt(numItems); // exclusive of numItems, inclusive
			// 0
			double rndInt = rnd.nextDouble() * normalizer; // normalize double
			// value
			if (rndInt < probIN[itemNum]) {
				found = true;
			}
		}
		return itemNum;
	}

	private void calculateConstAndProbs() {
		// try {
		// first do constant, C
		// C = 1/(sum(i=i to N) 1/i^exp)
		double denominator = 0;
		for (int i = 1; i < numItems + 1; i++) {
			denominator += (1 / StrictMath.pow(i, exp));
		}
		C = 1.0 / denominator;

		// FileWriter fw = new FileWriter(name + ".distr");
		for (int i = 0; i < numItems; i++) {
			probIN[i] = (1 / Math.pow(i + 1, exp)) * C; // probability of
			// itemNum
			// fw.write(String.valueOf(probIN[i]) + "\n");
		}
		normalizer = probIN[0];
		// fw.close();
		// } catch (IOException ioe) {
		// System.err.println("KT problem with distr file (" + name + "): " +
		// ioe.getMessage());
		// }
	}

}
