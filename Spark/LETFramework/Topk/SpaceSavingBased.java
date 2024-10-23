package LETFramework.Topk;

import javafx.util.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SpaceSavingBased extends TopkPart implements Serializable {
    private int bucketNum;
    private Map<Integer, BucketNode> hsh;
    private BucketNode val_nodes;
    private BucketValNode tail;
    private Bucket[] buckets;
    private int count;
    public SpaceSavingBased(int memInBytes) {
        bucketNum = memInBytes/Bucket.SIZE;
        buckets = new Bucket[bucketNum];
        hsh = new HashMap<>();
        for (int i = 0; i < bucketNum; i++) {
            buckets[i]=new Bucket();
        }
    }
    @Override
    public Pair<Integer, Pair<Integer, Integer>> insert(Integer key, Integer val) {
        if (hsh.containsKey(key)) {
            BucketNode bucket = hsh.get(key);
            for (int i = 0; i < Bucket.COUNTER_PER_BUCKET; i++) {
                if (bucket.bucket.getKey(i) == key) {
                    bucket.bucket.setVal(i, bucket.bucket.getVal(i) + 1);
                    break;
                }
            }
            addBucket(bucket);
            return new Pair<>(0,new Pair<>(0,0));
        } else {
            return appendNewKey(key,val);
        }
    }

    @Override
    public Map<Integer, Integer> queryAll() {
        Map<Integer, Integer> mp = new HashMap<>();
        for (BucketValNode SCounter = tail; SCounter != null; SCounter = SCounter.next) {
            for (BucketNode bucket = SCounter.first; bucket != null; bucket = bucket.next) {
                for (int index = 0; index < Bucket.COUNTER_PER_BUCKET; index++) {
                    mp.put(bucket.bucket.getKey(index), bucket.bucket.getVal(index));
                }
            }
        }
        return mp;
    }

    @Override
    public Integer query(Integer key) {
        if (hsh.containsKey(key)) {
            BucketNode bucket = hsh.get(key);
            for (int i = 0; i < Bucket.COUNTER_PER_BUCKET; i++) {
                if (bucket.bucket.getKey(i) == key) {
                    return bucket.bucket.getVal(i);
                }
            }
        }
        return 0;
    }


    private void addBucket(BucketNode bucket) {
        if (bucket.parent.next == null || bucket.parent.next.val != bucket.parent.val + 1) {
            BucketValNode ptr = new BucketValNode();
            ptr.val = bucket.parent.val + 1;
            ptr.next = bucket.parent.next;
            if (ptr.next != null)
                ptr.next.prev = ptr;
            ptr.prev = bucket.parent;
            bucket.parent.next = ptr;
        }
        BucketValNode initValNode = bucket.parent;
        BucketValNode newValNode = bucket.parent.next;
        if (bucket.next != null)
            bucket.next.prev = bucket.prev;
        if (bucket.prev != null)
            bucket.prev.next = bucket.next;
        if (bucket.prev == null) {
            initValNode.first = bucket.next;
        }
        bucket.next = newValNode.first;
        if (newValNode.first != null)
            newValNode.first.prev = bucket;
        bucket.prev = null;
        newValNode.first = bucket;
        bucket.parent = newValNode;
        if (initValNode.first == null) {
            if (initValNode.next != null)
                initValNode.next.prev = initValNode.prev;
            if (initValNode.prev != null)
                initValNode.prev.next = initValNode.next;
            if (initValNode == tail) {
                tail = initValNode.next;
            }
            initValNode = null; // Mark for garbage collection
        }
    }

    private Pair<Integer, Pair<Integer, Integer>> appendNewKey(int key, int f) {
        BucketNode bucket = tail.first;
        int min_val = bucket.bucket.getVal(0);
        int pos = 0;
        for (int i = 0; i < Bucket.COUNTER_PER_BUCKET; i++) {
            if (getCounterVal(bucket.bucket.getVal(i)) < min_val) {
                min_val = getCounterVal(bucket.bucket.getVal(i));
                pos = i;
            }
        }
        int swap_key = bucket.bucket.getKey(pos),swap_val = bucket.bucket.getVal(pos);
        if (swap_key != 0) {
            hsh.remove(swap_key);
            bucket.bucket.setVal(pos, f);
        } else {
            bucket.bucket.setVal(pos, f);
        }
        bucket.bucket.setKey(pos, key);
        hsh.put(key, bucket);
        addBucket(tail.first);
        return new Pair<>((swap_val > 0) ? 1 : 0,new Pair<>(swap_key,swap_val));
    }

    @Override
    public void initial() {
        count = 0;
        super.initial();
        tail = new BucketValNode();
        BucketNode ptr = new BucketNode();
        ptr.bucket = buckets[0];
        ptr.parent = tail;
        tail.first = ptr;
        for (int i = 1; i < bucketNum; i++) {
            BucketNode ptr1 = new BucketNode();
            ptr.next = ptr1;
            ptr1.prev = ptr;
            ptr1.parent = tail;
            ptr1.bucket = buckets[i];
            ptr = ptr1;
        }
    }

    @Override
    public void initial(TopkPart _refer) {
        super.initial(_refer);
        initial();
    }




    // BucketValNode class definition
    private class BucketValNode {
        public BucketValNode prev = null;
        public BucketValNode next = null;
        public BucketNode first = null;
        public int val = 0;
    }

    // BucketNode class definition
    private class BucketNode {
        public BucketNode prev = null;
        public BucketNode next = null;
        public BucketValNode parent = null;
        public Bucket bucket;
    }
}
