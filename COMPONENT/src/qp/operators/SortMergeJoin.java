/**
 * Page Nested Join algorithm
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import javax.sound.midi.SysexMessage;
import java.io.*;
import java.util.ArrayList;

public class SortMergeJoin extends Join {

    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    Batch leftbatch;                // Buffer page for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer

    MergeSort sortedLeft;
    MergeSort sortedRight;
    int leftPointer;
    int rightPointer;
    ArrayList<Tuple> trackDups = new ArrayList<>();


    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    public boolean open() {
        System.out.println("In sortmerge join");
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            System.out.println("left attr is: " + leftattr + "with index: " + left.getSchema().indexOf(leftattr));
            Attribute rightattr = (Attribute) con.getRhs();
            System.out.println("right attr is: " + rightattr + "with index: " + right.getSchema().indexOf(rightattr));
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }
        System.out.println("in smj, numbuff is: " + numBuff);

        System.out.println("=====left schema====");
        Debug.PPrint(left.getSchema());
        System.out.println();
        //System.out.println("sorting left");
        sortedLeft = new MergeSort(left, leftindex, optype, numBuff, "left");
        //System.out.println("DONE SORTING LEFT");


        System.out.println("=====right schema====");
        Debug.PPrint(right.getSchema());
        System.out.println();
        //System.out.println("sorting right");
        sortedRight = new MergeSort(right, rightindex, optype, numBuff, "right");
        //System.out.println("DONE SORTING RIGHT");
        //System.out.println("first tuple in sortedRight is: " + sortedRight.next().get(0));


        //System.out.println("first tuple in sortedLedt is: " + sortedLeft.next().get(0));

        /** initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;


        if (!sortedLeft.open() || !sortedRight.open()) {
            return false;
        }


        leftbatch = sortedLeft.next();
        //Tuple fromLeft = leftbatch.get(0);
        rightbatch = sortedRight.next();
        leftPointer = 0;
        rightPointer = 0;

        return true;
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/

    //Wwhile left and right not null, read in batch, take a value from right and match with left
    //if match, track the cursor position of the first matched value scan through entire left batch, if dup values end WITHIN bahc, jsut compare and output
    //then move onto next tuple on right. If same as top, backtrack, else search through left for match
    //if matched value matches the ENTIRE batch, check if next value on right is same as curr, if yes, write the left batch to file, read in next one write till all dups written
    // if no, then just read and match and output and get next left batch
    public Batch next() {
        System.out.println("in smj next");

        outbatch = new Batch(batchsize);
        System.out.println("Size of leftbatch is:" + leftbatch.size());
        //System.out.println("first tuple in leftbatch is: " + leftbatch.get(0)._data);

        System.out.println("Size of rightbatch is:" + rightbatch.size());
        //System.out.println("first tuple in rightbatch is: " + rightbatch.get(0)._data);
        //Tuple fromRight = rightbatch.get(0);
        //rightTrack.add(0);

        Tuple prevTuple = null;

        while (leftbatch.size() != 0 && rightbatch.size() != 0) {
            while (Tuple.compareTuples(leftbatch.get(leftPointer), rightbatch.get(rightPointer), leftindex, rightindex) >= 0) {
                //left bigger than right, go down right
                //System.out.println("start of while, right pointer is: " + rightPointer);

                Tuple fromLeft = leftbatch.get(leftPointer);
                Tuple toCompare = rightbatch.get(rightPointer);
                System.out.println("in left >= right: left tuple is: " + fromLeft.dataAt(leftindex.get(0)) + ", right tuple is: " + toCompare.dataAt(rightindex.get(0)));

                if (rightPointer != 0) {
                    Tuple prevTup = rightbatch.get(rightPointer - 1);
                    boolean same = true;
                    for (int i=0; i<rightindex.size(); i++) {
                        if (toCompare.dataAt(rightindex.get(i)) == prevTup.dataAt(rightindex.get(i))) {
                            same = same && true;
                        } else {
                            same = false;
                        }
                    }
                    if (same) {
                        //prev tuple and current got same join attr values
                        System.out.println("reading from trackdups");
                        //same as prev
                        for (int i = 0; i < trackDups.size(); i++) {
                            outbatch.add(fromLeft.joinWith(trackDups.get(i)));
                            System.out.println("joined in td");
                        }

                        leftPointer++;
                        if (leftPointer == leftbatch.size()) {
                            leftbatch = sortedLeft.next();
                            leftPointer = 0;
                        }

                    } else {
                        trackDups.clear();

                        if (Tuple.compareTuples(fromLeft, toCompare, leftindex, rightindex) == 0) {
                            outbatch.add(fromLeft.joinWith(toCompare));
                            trackDups.add(toCompare);
                            //rightPointer = i;
                            System.out.println("joined");
                        }
                    }
                } else {
                    trackDups.clear();

                    if (Tuple.compareTuples(fromLeft, toCompare, leftindex, rightindex) == 0) {
                        outbatch.add(fromLeft.joinWith(toCompare));
                        trackDups.add(toCompare);
                        //rightPointer = i;
                        System.out.println("joined");
                    }
                }


                rightPointer++;

                if (rightPointer == rightbatch.size()) {
                    rightbatch = sortedRight.next();
                    rightPointer = 0;
                }

                if (outbatch.isFull()) return outbatch;

                if (rightbatch == null || rightbatch.size() == 0) break;
                //System.out.println("end of while, right pointer is " + rightPointer);

            }


            while (Tuple.compareTuples(leftbatch.get(leftPointer), rightbatch.get(rightPointer), leftindex, rightindex) <= 0) {
                //right bigger than left, go down left
                //System.out.println("start of while, left pointer is: " + leftPointer);
                Tuple fromRight = rightbatch.get(rightPointer);
                Tuple toCompare = leftbatch.get(leftPointer);
                System.out.println("in left < right: left tuple is: " + toCompare.dataAt(leftindex.get(0)) + ", right tuple is: " + fromRight.dataAt(rightindex.get(0)));

                if (leftPointer != 0) {
                    //find out if the join values are the same
                    Tuple prevTup = leftbatch.get(leftPointer - 1);
                    boolean same = true;
                    for (int i=0; i<rightindex.size(); i++) {
                        if (toCompare.dataAt(leftindex.get(i)) == prevTup.dataAt(leftindex.get(i))) {
                            same = same && true;
                        } else {
                            same = false;
                        }
                    }

                    if (same) {
                        System.out.println("reading from trackdups");
                        //same as prev
                        for (int i = 0; i < trackDups.size(); i++) {
                            outbatch.add(fromRight.joinWith(trackDups.get(i)));
                            System.out.println("joined in td");

                        }
                        rightPointer++;
                        if (rightPointer == rightbatch.size()) {
                            rightbatch = sortedRight.next();
                            rightPointer = 0;
                        }
                    } else {
                        trackDups.clear();

                        if (Tuple.compareTuples(fromRight, toCompare, leftindex, rightindex) == 0) {
                            outbatch.add(fromRight.joinWith(toCompare));
                            trackDups.add(toCompare);
                            System.out.println("joined");
                            //rightPointer = i;
                        }
                    }
                } else {
                    trackDups.clear();

                    if (Tuple.compareTuples(fromRight, toCompare, leftindex, rightindex) == 0) {
                        outbatch.add(fromRight.joinWith(toCompare));
                        trackDups.add(toCompare);
                        System.out.println("joined");
                        //rightPointer = i;
                    }
                }

                leftPointer++;

                if (leftPointer == leftbatch.size()) {
                    leftbatch = sortedLeft.next();
                    leftPointer = 0;
                }

                if (outbatch.isFull()) return outbatch;

                //System.out.println("left batch is: " + leftbatch);
                if (leftbatch == null || leftbatch.size() == 0) break;
                //System.out.println("end of while, left pointer is " + leftPointer);
            }
        }

        if (outbatch.isEmpty()) {
            close();
            return null;
        }

        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        sortedRight.close();
        sortedLeft.close();
        return true;
    }

}
