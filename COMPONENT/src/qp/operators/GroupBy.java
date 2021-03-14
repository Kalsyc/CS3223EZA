package qp.operators;

import java.util.ArrayList;
import qp.utils.Attribute;

public class GroupBy extends MergeSort {

    public GroupBy(Operator base, ArrayList<Attribute> as, int type) {
        super(base, as, type, 4);
    }

	public ArrayList<Attribute> getProjAttr() {
        return attrSet;
    }

} 