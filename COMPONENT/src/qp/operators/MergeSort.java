package qp.operators;

import qp.utils.*;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

public class MergeSort extends Operator {
    protected Operator base;
    protected int numBuff;

    //for comparison
    //protected ArrayList<Attribute> attrSet;
    protected ArrayList<Integer> attrIndexArr;
    protected boolean isDesc;

    //used in generate sorted runs
    private int numRuns = 0;
    private int track = 0;
    protected int batchSize;

    //opening and closing
    boolean eos;     // Indicate whether end of stream is reached or not
    Batch outbatch;  // This is the current output buffer
    Tuple inComing;
    ArrayList<TupleReader> sortedRunReaders;
    protected ArrayList<String> sortedRuns; //filenames of sorted runs

    protected String fileName = "mergesort";
    //final file reader
    protected TupleReader out;

    /* MergeSort(Operator base, Vector as, int opType) {
        super(opType);
        this.base = base;
        this.attrSet = as;
//        this.numBuff = numBuff;
//        this.fileName = fileName;
    }*/

    public MergeSort(Operator base, ArrayList<Integer> as, int type, int numBuff, String fileName) {
        super(type);
        this.base = base;
        this.attrIndexArr = as;
        this.isDesc = false;
        this.numBuff = numBuff;
        this.fileName = fileName;
    }

    public MergeSort(Operator base, ArrayList<Integer> as, int type, int numBuff) {
        super(type);
        this.base = base;
        this.attrIndexArr = as;
        this.isDesc = false;
        this.numBuff = numBuff;
    }

    public MergeSort(Operator base, ArrayList<Integer> as, int type, int numBuff, boolean isDesc) {
        super(type);
        this.base = base;
        this.attrIndexArr = as;
        this.numBuff = numBuff;
        this.isDesc = isDesc;
    }


    /*public MergeSort(Operator base, ArrayList<Attribute> as, int type, int numBuff) {
        super(type);
        this.base = base;
        this.attrSet = as;
        this.isDesc = false;
    }*/

    //read in the tuples, generate sorted runs, do the merge and write
    public boolean open() {
        System.out.println("SortMerge:-----------------in open--------------");
        System.out.println("schema for this join is");
        Debug.PPrint(base.getSchema());

        System.out.println("Number of buffers is: " + numBuff);
        eos = false;  // Since the stream is just opened

        /** Set number of tuples per page**/
        int tuplesize = base.getSchema().getTupleSize();
        batchSize = Batch.getPageSize() / tuplesize;

        System.out.println("Max batch size is " + batchSize);

        if (!base.open()) {
            return false;
        } else {
            //get all the attributes for use in tuple comparison later
            //if (attrIndexArr == null) {

                // Phase 1: Generate sorted runs
                generateSortedRuns();

                // Phase 2: Merge sorted runs
                mergeSortedRuns();

                if (sortedRuns.size() != 1) {
                    return false;
                }
                out = new TupleReader(sortedRuns.get(0), batchSize);
                out.open();
                //debugging, printing all
            /*for (int i=0; i<124; i++) {
                Tuple next = out.next();
                if (next == null) {
                    System.out.println("!!!!!!!!null is at id: " + i);
                } else {
                    System.out.println(next._data);
                }
            }*/
            }
        return true;
    }

    //next has to return the next set of SORTED tuples --> meaning calling open will have to sort the tuples
    public Batch next() {
        if (sortedRuns.size() != 1) {
            System.out.println("There is something wrong with sort-merge process. ");
        }

        if (eos) {
            close();
            return null;
        }

        /** An output buffer is initiated **/
        outbatch = new Batch(batchSize);

        /** keep on checking the incoming pages until
         ** the output buffer is full
         **/
        while (!outbatch.isFull()) {
            inComing = out.next();
            //System.out.println("in next of sm, incoming is: " + inComing._data);
            /** There is no more incoming pages from base operator **/
            if (inComing == null) {
                eos = true;
                return outbatch;
            } else {
                outbatch.add(inComing);
            }
        }
        return outbatch;

    }

    public boolean close() {
        File toDelete = new File(sortedRuns.get(0));
        toDelete.delete();

        //close reader
        if (sortedRunReaders != null) {
            for (int i=0; i<sortedRunReaders.size(); i++) {
                sortedRunReaders.get(i).close();

            }
        }

        out.close();
        return true;
    }

    public void generateSortedRuns() {
        //an arraylist of tuples that fit into B buffers
        //System.out.println("Generating sorted runs");
        sortedRuns = new ArrayList<>();
        Batch nextBatch = base.next();
        ArrayList<Tuple> tuplesInRun = new ArrayList<>();

        while (nextBatch != null) {
            int numtuples = 0;
            tuplesInRun.clear();
            //one sorted run can have (batch x num buffers) of tuples
            for (int i = 0; i < numBuff; i++) {
               // System.out.println("Batch is: " + nextBatch);
                if (nextBatch != null) {
                    //System.out.println("Batch size is " + nextBatch.size());

                    for (int j = 0; j < nextBatch.size(); j++) {
                        //System.out.println("Batch number: " + i + " | tuple is " + nextBatch.get(j) + " with data: " + nextBatch.get(j)._data);
                        tuplesInRun.add(nextBatch.get(j));
                        numtuples++;
                    }
                }

                nextBatch = base.next();
                // size of batch here is 5
                //System.out.println("Next batch size is: " + nextBatch.size());
            }

            //System.out.println("tuplesinrun size is: " + tuplesInRun.size() + " Num tuples size is: " + numtuples);
            //Sort the tuples in the run
            if (isDesc) {
                Collections.sort(tuplesInRun, new AttributeSort(attrIndexArr, isDesc));
            } else {
                Collections.sort(tuplesInRun, new AttributeSort(attrIndexArr));
            }

            //Write all these to a temp file called mergesort-MSG-(num)
            String tempFileName = fileName + "-MSG-" + numRuns;
            TupleWriter writeTempFile = new TupleWriter(tempFileName, batchSize);
            writeTempFile.open();

            for (int i = 0; i < numtuples; i++) {
                writeTempFile.next(tuplesInRun.get(i));
            }

            //System.out.println("Number of tuples written is: " + writeTempFile.getNumTuple());
            //System.out.println("Tuples are written to: " + tempFileName);
            System.out.println();
            writeTempFile.close();

            //debugging
            /*System.out.println("!!!CHECKING TUPLES IN FILE FOR DEBUGGING!!!");
            TupleReader temp = new TupleReader(tempFileName, batchSize);
            temp.open();
            for (int i=0; i<numtuples; i++) {
                System.out.println(temp.next()._data);
            }
            temp.close();*/
            //close debugging

            sortedRuns.add(tempFileName);
            numRuns++;
        }

        /*System.out.println("Sorted runs are:");
        for (int i = 0; i < sortedRuns.size(); i++) {
            System.out.println(sortedRuns.get(i));
        }*/

    }

    /**
     * This is the merge part of the whole sort-merge process
     * Recursively merge until there are only one run
     */
    public void mergeSortedRuns() {
        //iterate through all the runs and recrusively merge them
        System.out.println("-----------------Merging sorted runs-----------------");

        //only can merge these number of runs at one time
        int numInputBuff = numBuff - 1; //1 for output buffer
        ArrayList<String> toMerge = new ArrayList<>();
        //System.out.println("initial number of sorted runs is: " + sortedRuns.size());

        while (sortedRuns.size() > 1) {
            for (int i = 0; i < numInputBuff; i++) {
                if (i < sortedRuns.size()) {
                    toMerge.add(sortedRuns.get(i));
                }
            }

            //System.out.println("The runs to merge are");
            for (int i = 0; i < toMerge.size(); i++) {
                //System.out.println(toMerge.get(i));
                sortedRuns.remove(toMerge.get(i));
            }

            String mergedFile = merge(toMerge);
            /*System.out.println("!!!!!JUST MERGED FILE");
            TupleReader temp = new TupleReader(mergedFile, batchSize);
            temp.open();
            //debugging, printing all
            for (int i=0; i<30; i++) {
                Tuple next = temp.next();
                if (next == null) {
                    System.out.println("!!!!!!!!null is at id: " + i);
                } else {
                    System.out.println(next._data);
                }
            }*/
            sortedRuns.add(mergedFile);

            //deleting sorted runs
            for (int i = 0; i < toMerge.size(); i++) {
                File toDelete = new File(toMerge.get(i));
                toDelete.delete();
            }
            toMerge.clear();
            //System.out.println("after one loop! number of sorted runs is: " + sortedRuns.size());
        }

        System.out.println("End of merging sorted runs. Number of runs is: " + sortedRuns.size());
    }

    public String merge(ArrayList<String> toMerge) {
        //System.out.println("----------------In intermediate merge------------------");
        int numRuns = toMerge.size();
        //System.out.println("numruns to merge is: " + numRuns);
        sortedRunReaders = new ArrayList<>();

        //preparing output file to write to
        String resultFileName = fileName + "-MS-" + track;
        TupleWriter writeTempFile = new TupleWriter(resultFileName, batchSize);
        writeTempFile.open();

        Batch outputBuffer = new Batch(batchSize);
        //set up to read from sorted files
        for (int i = 0; i < numRuns; i++) {
            //System.out.println("reading from: " + toMerge.get(i));
            TupleReader tupread = new TupleReader(toMerge.get(i), batchSize);
            tupread.open();
            sortedRunReaders.add(tupread);
        }

        //hashmap to map the index of the data to each run
        //get data from sorted runs and sort
        ArrayList<Tuple> inputBuffers = new ArrayList<>();
        //key is run number
        HashMap<Tuple, Integer> trackTuple = new HashMap<>();
        //initial filling of buffer //not utilising full buffer if num buff > num runs left
        for (int i = 0; i < numRuns; i++) {
            //System.out.println("Initial filling of buffer");
            //System.out.println("double check READING FROM FILE: " + sortedRunReaders.get(i).getFileName());
            for (int j = 0; j < batchSize; j++) {
                //wtf this reading is wrong why the fuck
                Tuple currTuple = sortedRunReaders.get(i).next();
                if (currTuple != null) {
                    inputBuffers.add(currTuple);
                    trackTuple.put(currTuple, i);
                }
                //System.out.println("Curr tuple is: " + currTuple + " with data: " + currTuple._data);

            }
        }

        //System.out.println("Done with intial buffer fill");
        //number of tuples in intial buffer should be numRuns x max batch size
        /*System.out.println("Number of tuples in input buffer is " + inputBuffers.size());
        System.out.println("---------------Printing tuples in input buffer--------------");
        for (int i = 0; i < inputBuffers.size(); i++) {
            System.out.println(inputBuffers.get(i)._data);
        }
        System.out.println("---------------Done--------------");*/
        /*System.out.println("Size of hashmap " + inputBuffers.size());
        System.out.println("---------------Printing hashmap--------------");
        System.out.println(trackTuple.values());
        System.out.println("---------------Done--------------");*/

        //Got an arraylist of tuples from each run, and hashmap that hashes each tuple to the run
        //do this until no more tuple in input buffer
        while (!inputBuffers.isEmpty()) {
            Tuple smallestTuple;
            if (isDesc) {
                smallestTuple = Collections.min(inputBuffers, new AttributeSort(attrIndexArr, true));
            } else {
                smallestTuple = Collections.min(inputBuffers, new AttributeSort(attrIndexArr));
            }
            

            //System.out.println("Smallest tuple is: " + smallestTuple._data);
            outputBuffer.add(smallestTuple);
            inputBuffers.remove(smallestTuple);

            //remove it from hashmap
            trackTuple.remove(smallestTuple);

            //check if any input buffers have been zero-ed yet, if yes read in new batch
            for (int i = 0; i < numRuns; i++) {
                if (!trackTuple.containsValue(i)) {
                    //System.out.println("There is an empty input buffer");
                    //all input inside the buffer for this run is transferred out
                    for (int j = 0; j < batchSize; j++) {
                        Tuple currTuple = sortedRunReaders.get(i).next();
                        if (currTuple != null) {
                            inputBuffers.add(currTuple);
                            trackTuple.put(currTuple, i);
                        }
                    }
                }
            }

            //check if output buffer needs to be written out
            if (outputBuffer.isFull()) {
                for (int i = 0; i < outputBuffer.size(); i++) {
                    writeTempFile.next(outputBuffer.get(i));
                }
                outputBuffer.clear();
            }
        }

        //System.out.println("Out of while-loop, num tuples in input buffer is: " + inputBuffers.size());

        //left the records in output buffer to be written
        if (!outputBuffer.isEmpty()) {
            for (int i = 0; i < outputBuffer.size(); i++) {
                writeTempFile.next(outputBuffer.get(i));
            }
        }

        outputBuffer.clear();
        trackTuple.clear();
        writeTempFile.close();

        track++;
        return resultFileName;
    }

    class AttributeSort implements Comparator<Tuple> {
        private ArrayList<Integer> attrIndexArr;
        private boolean isDesc;

        public AttributeSort(ArrayList<Integer> attrIndexArr, boolean isDesc) {
            this.attrIndexArr = attrIndexArr;
            this.isDesc = isDesc;
        }

        public AttributeSort(ArrayList<Integer> attrIndexArr) {
            this.attrIndexArr = attrIndexArr;
        }

        // Used for sorting of tuples using diff attributes
        public int compare(Tuple a, Tuple b) {
            int diff = 0;
            if (a == null || b == null) {
                return 1;
            } else {
                for (int i = 0; i < attrIndexArr.size(); i++) {
                    if (isDesc) {
                        diff = Tuple.compareTuples(b, a, attrIndexArr.get(i));
                    } else {
                        diff = Tuple.compareTuples(a, b, attrIndexArr.get(i));
                    }
                    //return the first successful comparison made
                    if (diff != 0) {
                        return diff;
                    }
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