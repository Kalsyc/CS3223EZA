package qp.operators;
import qp.utils.*;
import java.util.ArrayList;
import qp.utils.Attribute;

public class GroupBy extends Operator {

    Operator base;
    int numBuff;
    int batchsize;
    protected ArrayList<Attribute> as;
    protected MergeSort ms;

    boolean eos;
    Batch inbatch;
    Batch outbatch;
    int start;
    Tuple last;

    public GroupBy(Operator base, ArrayList<Attribute> groupList, int type) {
        super(type);
        this.base = base;
        this.as = groupList;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public ArrayList<Attribute> getGroupList() {
        return as;
    }

    public void setNumBuff(int numBuff) {
        this.numBuff = numBuff;
    }

    public boolean open() {
        last = null;
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        Schema baseSchema = base.getSchema();
        ArrayList<Integer> attrIndex= new ArrayList<>();
        for (int i = 0; i < as.size(); i++) {
            Attribute attr = (Attribute) as.get(i);
            int index = baseSchema.indexOf(attr);
            attrIndex.add(index);
        }

        ms = new MergeSort(base, attrIndex, OpType.GROUPBY, numBuff);
        ms.setSchema(base.getSchema());
        ms.setNumBuff(4);
        if (!ms.open()) return false;
        return true;
    }

    public Batch next() {
        System.out.println("-------------_For Group By ------------");
        if (last != null) {
            System.out.println("last is: " + last._data);
        } else {
            System.out.println("First call of group by");
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
        GroupBy newGroupBy = new GroupBy(newbase, as, optype);
        Schema newSchema = (Schema) newbase.getSchema().clone();
        newGroupBy.setSchema(newSchema);
        return newGroupBy;
    }

}