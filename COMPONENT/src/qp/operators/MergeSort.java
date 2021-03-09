package qp.operators;

import qp.utils.*;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

public class MergeSort extends Operator {
    protected Operator base;
    protected int numBuff;
    protected ArrayList<Attribute> attrSet;
    protected int[] attrIndexArr;
    protected ArrayList<Batch> singularRun;
    protected ArrayList<Tuple> tuplesInRun;
    private int numRuns = 0;
    private int track = 0;
    protected int batchSize;
    ObjectOutputStream out;            // Output file stream

    private String fileName = "mergesort";

    protected ArrayList<String> sortedRuns; //filenames of sorted runs

    protected ObjectInputStream in;

    /* MergeSort(Operator base, Vector as, int opType) {
        super(opType);
        this.base = base;
        this.attrSet = as;
//        this.numBuff = numBuff;
//        this.fileName = fileName;
    }*/

    public MergeSort(Operator base, ArrayList<Attribute> as, int type, int numBuff) {
        super(type);
        this.base = base;
        this.attrSet = as;
    }

    //read in the tuples, generate sorted runs, do the merge and write
    public boolean open() {
        System.out.println("SortMerge:-----------------in open--------------");
        System.out.println("Number of buffers is: " + numBuff);
        //eos = false;  // Since the stream is just opened
        //start = 0;    // Set the cursor to starting position in input buffer

        /** Set number of tuples per page**/
        int tuplesize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tuplesize;
        //omg wtf is batch size...
        System.out.println("Max batch size is " + batchSize);
        if (!base.open()) {
            return false;
        } else {
            //get all the attributes for use in tuple comparison later
            Schema baseSchema = base.getSchema();
            attrIndexArr = new int[attrSet.size()];
            for (int i = 0; i < attrSet.size(); i++) {
                Attribute attr = (Attribute) attrSet.get(i);
                int index = baseSchema.indexOf(attr);
                attrIndexArr[i] = index;
            }

            // Phase 1: Generate sorted runs
            //sortedFiles = new ArrayList<>();
//            System.out.println("generate sort runs");
            generateSortedRuns();

            // Phase 2: Merge sorted runs
//            System.out.println("merge sort run: ");
            mergeSortedRuns();

//            testResultFile(sortedFiles.get(0));

            try {
//                File file = new File(fileName);
//                file = sortedFiles
                if (sortedRuns.size() != 1) {
                    return false;
                }
                in = new ObjectInputStream(new FileInputStream(sortedRuns.get(0)));
            } catch (IOException e) {
                System.out.println(" Error reading the file");
                return false;
            }
            return true;
        }
    }

    //next has to return the next set of SORTED tuples --> meaning calling open will have to sort the tuples
    public Batch next() {
//        System.out.println("SortMerge:-----------------in next--------------");
        if (sortedRuns.size() != 1) {
            System.out.println("There is something wrong with sort-merge process. ");
        }
        try {
            Batch batch = (Batch) in.readObject();
            return batch;
        } catch (IOException e) {
            return null;
        } catch (ClassNotFoundException e) {
            System.out.println("File not found. ");
        }
        return null;
    }

    public boolean close() {
        File toDelete = new File(sortedRuns.get(0));
        toDelete.delete();
        try {
            in.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
//        op.close();
        return true;
    }

    public void generateSortedRuns() {
        //an arraylist of tuples that fit into B buffers
        System.out.println("Generating sorted runs");
        int numtuples = 0;
        tuplesInRun = new ArrayList<>();
        sortedRuns = new ArrayList<>();
        Batch nextBatch = base.next();

        while (nextBatch != null) {
            //one sorted run can have (batch x num buffers) of tuples
            for (int i = 0; i < numBuff; i++) {
                System.out.println("Batch is: " + nextBatch);
                if (nextBatch != null) {
                    System.out.println("Batch size is " + nextBatch.size());

                    for (int j = 0; j < nextBatch.size(); j++) {
                        System.out.println("Batch number: " + i + " | tuple is " + nextBatch.get(j)._data);
                        tuplesInRun.add(nextBatch.get(j));
                        numtuples++;
                    }
                }

                nextBatch = base.next();
            }

            //Sort the tuples in the run
            Collections.sort(tuplesInRun, new AttributeSort(attrIndexArr));

            //Write all these to a temp file called smth-MSG0 (for eg)
            String tempFileName = fileName + "-MSG-" + numRuns;
            TupleWriter writeTempFile = new TupleWriter(tempFileName, numtuples);
            writeTempFile.open();

            for (int i = 0; i < numtuples; i++) {
                writeTempFile.next(tuplesInRun.get(i));
            }

            writeTempFile.close();
            System.out.println("temp file name is: " + tempFileName);
            sortedRuns.add(tempFileName);
            numRuns++;
        }

    }

    /**
     * This is the merge part of the whole sort-merge process
     * Recursively merge until there are only one run
     */
    public void mergeSortedRuns() {
        //iterate through all the runs and recrusively merge them
        System.out.println("Merging sorted runs");

        //only can merge these number of runs at one time
        int numInputBuff = numBuff - 1; //1 for output buffer
        ArrayList<String> toMerge = new ArrayList<>();
        System.out.println("initial number of sorted runs is: " + sortedRuns.size());

        while (sortedRuns.size() > 1) {
            for (int i = 0; i < numInputBuff; i++) {
                if (i < sortedRuns.size()) {
                    toMerge.add(sortedRuns.get(i));
                    sortedRuns.remove(i);
                }
            }

            String mergedFile = merge(toMerge);
            sortedRuns.add(mergedFile);
            System.out.println("after one loop! number of sorted runs is: " + sortedRuns.size());
        }

        System.out.println("End of merging sorted runs. Number of runs is: " + sortedRuns.size());
    }

    public String merge(ArrayList<String> tempRuns) {
        System.out.println("In intermediate merge");
        int numRuns = tempRuns.size();
        System.out.println("numruns is: " + numRuns);
        ArrayList<ObjectInputStream> sortedRunStreams = new ArrayList<>();

        String resultFileName = fileName + "-MS-" + track;
        ObjectOutputStream resultFile = null;
        try {
            resultFile = new ObjectOutputStream(new FileOutputStream(resultFileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        Batch outputBuffer = new Batch(batchSize);
        //read from sorted files
        for (int i = 0; i < numRuns; i++) {
            try {
                ObjectInputStream indivStream = new ObjectInputStream(new FileInputStream(tempRuns.get(i)));
                sortedRunStreams.add(indivStream);
            } catch (FileNotFoundException e) {
                System.out.println("cannot find file with name " + tempRuns.get(i));
                e.printStackTrace();
            } catch (IOException e) {
                System.out.println("error creating output stream");
                e.printStackTrace();
            }
        }
        //hashmap to map the index of the data to each run
        //get data from sorted runs and sort
        ArrayList<Tuple> inputBuffers = new ArrayList<>();
        //key is run number
        HashMap<Tuple, Integer> trackTuple = new HashMap<>();
        //initial filling of buffer //not utilising full buffer if num buff > num runs left
        for (int i = 0; i < numRuns; i++) {
            System.out.println("Initial filling of buffer");
            Batch currBatch = getNext(sortedRunStreams.get(i));
            //THIS IS FUNKY CUZ THE SIZE OF THIS BATCH IS MORE THAN IN HERE!! fix it
            int batchSize = currBatch.size();
            System.out.println("Initial filling: size of batch is: " + batchSize);
            for (int j = 0; j < batchSize; j++) {
                Tuple currTuple = currBatch.get(j);
                inputBuffers.add(currTuple);
                trackTuple.put(currTuple, i);
            }
        }

        System.out.println("Done with intial fill");
        //number of tuples in intiial buffer should be numRuns x max batch size
        System.out.println("Number of tuples in input buffer is " + inputBuffers.size());
        System.out.println("---------------Printing tuples in input buffer--------------");
        for (int i = 0; i < inputBuffers.size(); i++) {
            System.out.println(inputBuffers.get(i)._data);
        }
        System.out.println("---------------Done--------------");

        System.out.println("Size of hashmap " + inputBuffers.size());
        System.out.println("---------------Printing hashmap--------------");
        System.out.println(trackTuple.values());
        System.out.println("---------------Done--------------");

        //Got an arraylist of tuples from each run, and hashmap that hashes each tuple to the run
        //do this until no more tuple in input buffer
        while (!inputBuffers.isEmpty()) {
            Tuple smallestTuple = Collections.min(inputBuffers, new AttributeSort(attrIndexArr));
            System.out.println("Smallest tuple is: " + smallestTuple._data);
            outputBuffer.add(smallestTuple);
            inputBuffers.remove(smallestTuple);
            //remove from input buffer
            /*for (int i = 0; i < inputBuffers.size(); i++) {
                if (inputBuffers.get(i).equals(smallestTuple)) {
                    System.out.println("Removing tuple: " + inputBuffers.get(i)._data);
                    inputBuffers.remove(i);
                }
            }*/
            System.out.println("After removal, num of tups " + inputBuffers.size());
            //remove it from hashmap
            trackTuple.remove(smallestTuple);

            //check if any input buffers have been zero-ed yet, if yes read in new batch
            for (int i = 0; i < numRuns; i++) {
                if (!trackTuple.containsValue(i)) {
                    System.out.println("There is an empty input buffer");
                    //all input inside the buffer for this run is transferred out
                    //NEED TO CHECK IF
                    Batch currBatch = getNext(sortedRunStreams.get(i));
                    if (currBatch != null) {
                        int batchSize = currBatch.size();
                        for (int j = 0; j < batchSize; j++) {
                            Tuple currTuple = currBatch.get(j);
                            inputBuffers.add(currTuple);
                            trackTuple.put(currTuple, i);
                        }
                    }


                }
            }

            //check if output buffer needs to be written out
            if (outputBuffer.isFull()) {
                try {
                    resultFile.writeObject(outputBuffer);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                outputBuffer.clear();
            }
        }

        //left the records in output buffer to be written
        if (!outputBuffer.isEmpty()) {
            try {
                resultFile.writeObject(outputBuffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        outputBuffer.clear();
        trackTuple.clear();
        try {
            resultFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //might have to delete intermediate files
        track++;
        return resultFileName;
    }


    protected Batch getNext(ObjectInputStream inputStream) {
        Batch next = null;
        try {
            next = (Batch) inputStream.readObject();
            if (next.isEmpty()) {
                System.out.println("Batch is empty");
                return next;
            }
        } catch (IOException e) {
            return null;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return next;
    }

    public ObjectOutputStream initObjectOutputStream(File file) {
        try {
            return new ObjectOutputStream(new FileOutputStream(file, true));
        } catch (IOException io) {
            System.out.println("SortMerge: cannot initialize object output stream");
        }
        return null;
    }

    public void appendToObjectOutputStream(ObjectOutputStream out, Batch batch) {
        try {
            out.writeObject(batch);
            out.reset();          //reset the ObjectOutputStream to enable appending result

        } catch (IOException io) {
            System.out.println("SortMerge: encounter error when append to object output stream");
        }
    }

    public void closeObjectOutputStream(ObjectOutputStream out) {
        try {
            out.close();
        } catch (IOException io) {
            System.out.println("SortMerge: cannot close object output stream");
        }
    }

    class AttributeSort implements Comparator<Tuple> {
        private int[] attrIndexArr;

        public AttributeSort(int[] attrIndexArr) {
            this.attrIndexArr = attrIndexArr;
        }

        // Used for sorting of tuples using diff attributes
        public int compare(Tuple a, Tuple b) {
            int diff = 0;
            for (int i = 0; i < attrIndexArr.length; i++) {
                diff = Tuple.compareTuples(a, b, attrIndexArr[i]);
                //return the first successful comparison made
                if (diff != 0) {
                    return diff;
                }
            }
            return diff;
        }
    }

    /**
     * number of buffers available to this join operator
     **/

    public void setNumBuff(int num) {
        this.numBuff = num;
    }

    public int getNumBuff() {
        return numBuff;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }
}