package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

public class BlockNestedJoin extends Join {

	static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   				// Index of the join attribute in left table
    ArrayList<Integer> rightindex;  				// Index of the join attribute in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    Batch leftbatch;                // Buffer page for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached

    public BlockNestedJoin(Join jn) {
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

		/** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        //System.out.println("num buff is: " + numBuff + " batchsize is: " + batchsize);
        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }

		Batch rightpage;

        /** initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;
        eosl = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr = true;

		if (!right.open()) {
			return false;
		} else {
			filenum++;
            rfname = "BNJtemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("BlockNestedJoin: Error writing to temporary file");
                return false;
            }
            if (!right.close()) {
                return false;
			}
        }

        if (left.open()) {
            return true;
		} else {
            return false;
		}
	}

	/**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {
        //System.out.println("in next");
        int i, j;
        if (eosl) {
            close();
            return null;
        }
        outbatch = new Batch(batchsize);

        while (!outbatch.isFull()) {
            if (lcurs == 0 && eosr == true) {
                /** new left block is to be fetched**/
                ArrayList<Batch> leftblock = new ArrayList<>();
                for (int k = 0; k < numBuff - 2; k++) {
                    Batch batch = (Batch) left.next();
                    if (batch == null || batch.isEmpty()) {
                        break;
                    }
                    leftblock.add(batch);
                }

                if (leftblock.isEmpty()) {
                    eosl = true;
                    return outbatch;
                }

                leftbatch = new Batch(leftblock.size() * batchsize);
                for (Batch page: leftblock) {
                    for (int k = 0; k < page.size(); k++) {
                        leftbatch.add(page.get(k));
                    }
                }

                /*System.out.println("left batch size is: " + leftbatch.size());
                System.out.println("To debug----------------- Printing leftbatch");
                for (int p=0; p<leftbatch.size(); p++) {
                    System.out.println(leftbatch.get(p)._data);
                }*/

                try {
                    in = new ObjectInputStream(new FileInputStream(rfname));
                    eosr=false;
                } catch (IOException io) {
                    System.err.println("BlockNestedJoin:error in reading the file");
                    System.exit(1);
                }
 			}

            while (eosr == false) {
                try {
                    if (rcurs == 0 && lcurs == 0) {
                        //System.out.println("Reading new right batch in");
                        rightbatch = (Batch) in.readObject();
                        //System.out.println("Batch size is: " + rightbatch.size());
                    }
                    for (i = lcurs; i < leftbatch.size(); ++i) {

                        for (j = rcurs; j < rightbatch.size(); ++j) {
                            Tuple lefttuple = leftbatch.get(i);
                            Tuple righttuple = rightbatch.get(j);
                            /*System.out.println("------------------------------------");
                            System.out.println("Left tuple is: " + lefttuple._data);
                            System.out.println("Right tuple is: " + righttuple._data);*/

                            if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                //System.out.println("Joining");

                                Tuple outtuple = lefttuple.joinWith(righttuple);
                                //System.out.println("------------------------------------");

                                outbatch.add(outtuple);
                                if (outbatch.isFull()) {
                                    //System.out.println("outbatch is full");
                                    //System.out.println("outbatch size is: " + outbatch.size());
                                    if (i == leftbatch.size() - 1 && j == rightbatch.size() - 1) {  //case 1
                                        lcurs = 0;
                                        rcurs = 0;
                                    } else if (i != leftbatch.size() - 1 && j == rightbatch.size() - 1) {  //case 2
                                        lcurs = i + 1;
                                        rcurs = 0;
                                    } else if (i == leftbatch.size() - 1 && j != rightbatch.size() - 1) {  //case 3
                                        lcurs = i;
                                        rcurs = j + 1;
                                    } else {
                                        lcurs = i;
                                        rcurs = j + 1;
                                    }
                                    return outbatch;
                                }
                            }
                        }
                        rcurs = 0;
                    }
                    lcurs = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedJoin: Error in reading temporary file");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("BlockNestedJoin: Error in deserialising temporary file ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("BlockNestedJoin: Error in reading temporary file");
                    System.exit(1);
                }
            }
		}
        return outbatch;
	}

    /**
     * Close the operator
     */
    public boolean close() {
        File f = new File(rfname);
        f.delete();
        return true;
    }
}
