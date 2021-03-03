/**
 * Select Operation
 **/

package qp.operators;

import qp.utils.*;

import java.util.ArrayList;

public class Distinct extends Operator {

    Operator base;  // Base operator
    Condition con;  // Select condition
    int batchsize;  // Number of tuples per outbatch

    /**
     * The following fields are required during
     * * execution of the select operator
     **/
    boolean eos;     // Indicate whether end of stream is reached or not
    Batch inbatch;   // This is the current input buffer
    Batch outbatch;  // This is the current output buffer
    int start;       // Cursor position in the input buffer

    /**
     * constructor
     **/
    public Distinct(Operator base, int type) {
        super(type);
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * projected from the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;

        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        int i = 0;
        if (eos) {
            close();
            return null;
        }

        outbatch = new Batch(batchsize);
        /** all the tuples in the inbuffer goes to the output buffer **/
        //inbatch = base.next();

        /** keep on checking the incoming pages until
         ** the output buffer is full
         **/
        while (!outbatch.isFull()) {
            if (start == 0) {
                inbatch = base.next();
                /** There is no more incoming pages from base operator **/
                if (inbatch == null) {
                    eos = true;
                    return outbatch;
                }
            }

            /** Continue this for loop until this page is fully observed
             ** or the output buffer is full
             **/
            for (i = start; i < inbatch.size() && (!outbatch.isFull()); ++i) {
                Tuple present = inbatch.get(i);
                System.out.println(present._data);
                /** If the condition is satisfied then
                 ** this tuple is added to the output buffer
                 **/
                //check if tuple is present in outbatch
                if(!checkExists(present, outbatch)) {
                    outbatch.add(present);
                }
            }

            /** Modify the cursor to the position requierd
             ** when the base operator is called next time;
             **/
            if (i == inbatch.size())
                start = 0;
            else
                start = i;
        }


        return outbatch;
    }

    protected boolean checkExists(Tuple tuple, Batch outbatch) {
        for (int i=0; i<outbatch.size(); i++) {
            ArrayList compareTup = outbatch.get(i)._data;
            if (compareTup.equals(tuple._data)) {
                return true;
            }
        }
        return false;
    }
    /**
     * Close the operator
     */
    public boolean close() {
        inbatch = null;
        base.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        Distinct newDistinct = new Distinct(newbase, optype);
        Schema newSchema = (Schema) newbase.getSchema().clone();
        newDistinct.setSchema(newSchema);
        return newDistinct;
    }
}
