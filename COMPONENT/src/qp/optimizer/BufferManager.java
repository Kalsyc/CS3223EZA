/**
 * simple buffer manager that distributes the buffers equally among all the join operators
 **/

package qp.optimizer;

public class BufferManager {

    static int numBuffer;
    static int numJoin;

    static int buffPerJoin;

    public BufferManager(int numBuffer, int numJoin) {
        this.numBuffer = numBuffer;
        this.numJoin = numJoin;
        buffPerJoin = numBuffer / numJoin;
    }

    public BufferManager(int numBuffer) {
        this.numBuffer = numBuffer;
        this.numJoin = 0;
        buffPerJoin = 0;
    }

    public static int getNumBuffer() {
        return numBuffer;
    }

    public static int getNumJoin() {
        return numJoin;
    }

    public static int getBuffersPerJoin() {
        return buffPerJoin;
    }

}
