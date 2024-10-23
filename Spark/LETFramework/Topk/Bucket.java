package LETFramework.Topk;

public class Bucket {
    public static final int COUNTER_PER_BUCKET = 8;
    public static final int SIZE = COUNTER_PER_BUCKET * Integer.BYTES * 2;
    private int[] key;
    private int[] val;

    public Bucket() {
        key = new int[COUNTER_PER_BUCKET];
        val = new int[COUNTER_PER_BUCKET];
    }

    // Getter 和 Setter 方法
    public int getKey(int index) {
        return key[index];
    }

    public void setKey(int index, int value) {
        key[index] = value;
    }

    public int getVal(int index) {
        return val[index];
    }

    public void setVal(int index, int value) {
        val[index] = value;
    }
}
