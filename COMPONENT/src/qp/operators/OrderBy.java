package qp.operators;

import qp.utils.*;

import java.util.ArrayList;

public class OrderBy extends Operator {
  
    Operator base;
    int batchsize;
    boolean isDesc;

    Batch inbatch;
    Batch outbatch;

    //Index of the attributes in the base operator that are to be ordered by
    ArrayList<Attribute> attrList;

    public OrderBy(Operator base, boolean isDesc, ArrayList<Attribute> attrList, int type) {
        super(type);
        this.base = base;
        this.isDesc = isDesc;
        this.attrList = attrList;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public boolean getIsDesc() {
        return isDesc;
    }

    public void setIsDesc(boolean flag) {
        this.isDesc = flag;
    }

    public boolean open() {
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        return base.open();
    }

    public Batch next() {
        outbatch = new Batch(batchsize);
        inbatch = base.next();

        if (inbatch == null) {
            return null;
        }

        return base.next();

    }

    public boolean close() {
        inbatch = null;
        base.close();
        return true;
    }


}
