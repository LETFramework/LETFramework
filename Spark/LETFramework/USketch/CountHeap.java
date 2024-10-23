package LETFramework.USketch;

import java.util.*;

import common.BOBHash32;
import common.utils;

class KV {
    int key;
    int value;

    KV(int key, int value) {
        this.key = key;
        this.value = value;
    }
}

class VK implements Comparable<VK> {
    int value;
    int key;

    VK(int value, int key) {
        this.value = value;
        this.key = key;
    }

    @Override
    public int compareTo(VK other) {
        return Integer.compare(this.value, other.value);
    }
}

class CountHeap {


    private List<VK> heap;
    private int d;
    private int heapElementNum = 0;
    private int memInBytes = 0;
    private int w;
    private int[][] cmSketch;
    private int capacity;
    private int[] rnd1, rnd2;
    private BOBHash32[] hash, hashPolar;
    private Map<Integer, Integer> ht;

    public CountHeap(int cmMem, int heapMem, int d) {
        this.memInBytes = cmMem;
        this.heapElementNum = 0;
        this.d = d;
        this.capacity = heapMem / 4; // Assuming key_len is 4
        this.heap = new ArrayList<>(capacity);
        this.w = memInBytes / 4 / d;
        this.cmSketch = new int[d][w];
        this.rnd1 = new int[d];
        this.rnd2 = new int[d];
        this.hash = new BOBHash32[d];
        this.hashPolar = new BOBHash32[d];
        this.ht = new HashMap<>();

        Random rand = new Random();
        for (int i = 0; i < d; i++) {
            rnd1[i] = rand.nextInt(BOBHash32.MAX_PRIME32);
            rnd2[i] = rand.nextInt(BOBHash32.MAX_PRIME32);
            hash[i] = new BOBHash32(rnd1[i]);
            hashPolar[i] = new BOBHash32(rnd2[i]);
        }
    }

    public CountHeap(CountHeap refer) {
        this.memInBytes = refer.memInBytes;
        this.capacity = refer.capacity;
        this.heapElementNum = 0;
        this.d = refer.d;
        this.w = memInBytes / 4 / d;
        this.heap = new ArrayList<>(capacity);
        this.cmSketch = new int[d][w];
        this.rnd1 = Arrays.copyOf(refer.rnd1, refer.rnd1.length);
        this.rnd2 = Arrays.copyOf(refer.rnd2, refer.rnd2.length);
        this.hash = new BOBHash32[d];
        this.hashPolar = new BOBHash32[d];
        this.ht = new HashMap<>();

        for (int i = 0; i < d; i++) {
            hash[i] = new BOBHash32(rnd1[i]);
            hashPolar[i] = new BOBHash32(rnd2[i]);
        }
    }

    public double getF2() {
        double[] res = new double[d];
        for (int i = 0; i < d; ++i) {
            double est = 0;
            for (int j = 0; j < w; ++j) {
                est += Math.pow(cmSketch[i][j], 2);
            }
            res[i] = est;
        }

        Arrays.sort(res);
        if (d % 2 != 0) {
            return res[d / 2];
        } else {
            return (res[d / 2] + res[d / 2 - 1]) / 2;
        }
    }

    private void heapAdjustDown(int i) {
        while (i < heapElementNum / 2) {
            int lChild = 2 * i + 1;
            int rChild = 2 * i + 2;
            int largerOne = i;
            if (lChild < heapElementNum && heap.get(lChild).compareTo(heap.get(largerOne)) < 0) {
                largerOne = lChild;
            }
            if (rChild < heapElementNum && heap.get(rChild).compareTo(heap.get(largerOne)) < 0) {
                largerOne = rChild;
            }
            if (largerOne != i) {
                Collections.swap(heap, i, largerOne);
                ht.put(heap.get(i).key, i);
                ht.put(heap.get(largerOne).key, largerOne);
                heapAdjustDown(largerOne);
            } else {
                break;
            }
        }
    }

    private void heapAdjustUp(int i) {
        while (i > 0) {
            int parent = (i - 1) / 2;
            if (heap.get(parent).compareTo(heap.get(i)) <= 0) {
                break;
            }
            Collections.swap(heap, i, parent);
            ht.put(heap.get(i).key, i);
            ht.put(heap.get(parent).key, parent);
            i = parent;
        }
    }

    public int query(byte[] key) {
        int[] ans = new int[d];

        for (int i = 0; i < d; ++i) {
            int idx = (int)(hash[i].run(key) % w);
            int polar = (int)(hashPolar[i].run(key) % 2);
            int val = cmSketch[i][(int)idx];

            ans[i] = polar != 0 ? val : -val;
        }

        Arrays.sort(ans);

        int tmin;
        if (d % 2 == 0) {
            tmin = (ans[d / 2] + ans[d / 2 - 1]) / 2;
        } else {
            tmin = ans[d / 2];
        }
        return tmin;
    }

    public void insert(byte[] key, int f) {
        int[] ans = new int[d];

        for (int i = 0; i < d; ++i) {
            int idx = (int)(hash[i].run(key) % w);
            int polar = (int)(hashPolar[i].run(key) % 2);
            if (idx>w || idx<0)
            {
                int t=(int)(hash[i].run(key)%w);
            }
            cmSketch[i][idx] += polar != 0 ? f : -f;

            int val = cmSketch[i][idx];

            ans[i] = polar != 0 ? val : -val;
        }

        Arrays.sort(ans);

        int tmin;
        if (d % 2 == 0) {
            tmin = (ans[d / 2] + ans[d / 2 - 1]) / 2;
        } else {
            tmin = ans[d / 2];
        }
        tmin = Math.max(tmin, 1);
        Integer strKey = utils.ByteToInt(key);
        if (ht.containsKey(strKey)) {
            heap.get(ht.get(strKey)).value += f;
            heapAdjustDown(ht.get(strKey));
        } else if (heapElementNum < capacity) {
            heap.add(new VK(tmin, strKey));
            ht.put(strKey, heapElementNum++);
            heapAdjustUp(heapElementNum - 1);
        } else if (tmin > heap.get(0).value) {
            VK kv = heap.get(0);
            ht.remove(kv.key);
            kv.key = strKey;
            kv.value = tmin;
            ht.put(strKey, 0);
            heapAdjustDown(0);
        }
    }

    public void getTopKWithFrequency(List<KV> result) {
        result.clear();
        VK[] a = heap.toArray(new VK[0]);
        Arrays.sort(a);
        int total=Math.min(capacity,heapElementNum);
        for (int i = 0; i < total; ++i) {
            result.add(new KV(a[total-1-i].key, a[total-1-i].value));
        }
    }

    public void getL2HeavyHitters(double alpha, List<KV> result) {
        getTopKWithFrequency(result);
        double f2 = getF2();
        for (int i = 0; i < capacity; ++i) {
            if (Math.pow(result.get(i).value, 2) < alpha * f2) {
                result.subList(i, result.size()).clear();
                return;
            }
        }
    }

    public void getHeavyHitters(int threshold, List<Map.Entry<Integer, Integer>> ret) {
        ret.clear();
        for (int i = 0; i < capacity; ++i) {
            if (heap.get(i).value >= threshold) {
                ret.add(new AbstractMap.SimpleEntry<>(heap.get(i).key, heap.get(i).value));
            }
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        for (int i = 0; i < d; ++i) {
            hash[i] = null;
            hashPolar[i] = null;
        }
    }

    public static void join(CountHeap x, CountHeap y, CountHeap target) {
        for (int i = 0; i < x.d; i++) {
            for (int idx = 0; idx < x.w; idx++) {
                target.cmSketch[i][idx] = x.cmSketch[i][idx] + y.cmSketch[i][idx];
            }
        }
    }
}