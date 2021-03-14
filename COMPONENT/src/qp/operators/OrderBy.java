package qp.operators;
import qp.utils.*;
import java.util.ArrayList;

public class OrderBy extends Operator {
    
    Operator base;
    int numBuff;
    int batchsize;
    protected ArrayList<Attribute> as;
    protected MergeSort ms;
    protected boolean isDesc;

    boolean eos;
    Batch inbatch;
    Batch outbatch;
    int start;
    Tuple last;
    

    public OrderBy(Operator base, ArrayList<Attribute> orderList, boolean isDesc, int type) {
        super(type);
        this.base = base;
        this.as = orderList;
        this.isDesc = isDesc;
    }

    public OrderBy(Operator base, ArrayList<Attribute> orderList, int type) {
        super(type);
        this.base = base;
        this.as = orderList;
        this.isDesc = false;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public ArrayList<Attribute> getOrderList() {
        return as;
    }

    public void setNumBuff(int numBuff) {
        this.numBuff = numBuff;
    }

    public boolean getIsDesc() {
        return isDesc;
    }

    public boolean open() {
        last = null;
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        ms = new MergeSort(base, as, OpType.ORDERBY, numBuff, isDesc);
        ms.setSchema(base.getSchema());
        ms.setNumBuff(4);
        if (!ms.open()) return false;
        return true;
    }

    public Batch next() {
        System.out.println("-------------_For Order By ------------");
        if (last != null) {
            System.out.println("last is: " + last._data);
        } else {
            System.out.println("First call of order by");
        }
        int i = 0;
        if (eos) {
            close();
            return null;
        }

        outbatch = new Batch(batchsize);
        while (!outbatch.isFull()) {
            if (start == 0) {
                inbatch = ms.next();
                if (inbatch == null) {
                    eos = true;
                    return outbatch;
                }
            }

            for (i = start; i < inbatch.size() && (!outbatch.isFull()); i++) {
                Tuple present = inbatch.get(i);
                outbatch.add(present);
                last = present;
            }
        }
        
        return outbatch;
    }

    public boolean close() {
        inbatch = null;
        ms.close();
        base.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        OrderBy newOrderBy = new OrderBy(newbase, as, optype);
        Schema newSchema = (Schema) newbase.getSchema().clone();
        newOrderBy.setSchema(newSchema);
        return newOrderBy;
    }
}
