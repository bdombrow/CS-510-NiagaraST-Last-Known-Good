package niagara.firehose;

import java.util.*;

class Persons {

    public static double PERSON_EXP = 0.3; // exponent for person distribution
    public static int PERSON_DISTR_SIZE = 100;

    protected ZipfDistr personZipf = new ZipfDistr(PERSON_EXP, PERSON_DISTR_SIZE, "persons");

    // persons dont go away so lowChunk is always 0
    private int highChunk = 1;
    private int currId = 0;

    // scramble ids so that first person allocated isn't necessarily
    // the most active seller/bidder, etc.
    private int scrambler[] = new int[PERSON_DISTR_SIZE];
    private Random rnd =  new Random(283494);

    public Persons() {
	initializeScrambler();
    }

    // creates the open auction instance as well as returning the new id
    public int getNewId() {
	int newId = currId;
	currId++;
	if(newId == highChunk*PERSON_DISTR_SIZE) {
	    highChunk++;
	}
	return newId;
    }

    public int getExistingId() {  // used by generateBid 
	//int id = personZipf.nextInt();
	int id = rnd.nextInt(PERSON_DISTR_SIZE);
	//id = scrambler[id];
	id += getRandomChunkOffset();
	return id%currId;
    }

    private void initializeScrambler() {
	for(int i = PERSON_DISTR_SIZE-1; i>1; i--) {
	    int swapId = rnd.nextInt(i); // generates 0 to i - exclusive of i
	    int swapVal = scrambler[swapId];
	    scrambler[swapId] = scrambler[i];
	    scrambler[i] = swapVal;
	}
    }

    private int getRandomChunkOffset() {
	int chunkId = rnd.nextInt(highChunk);
	return chunkId*PERSON_DISTR_SIZE;
    }

}

