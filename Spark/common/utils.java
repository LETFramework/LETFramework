package common;

import javafx.util.Pair;
import scala.Int;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class utils {

    static int TUPLES_LEN = 13;

    public static class DATA_TYPE implements Serializable {
        public byte[] array = new byte[TUPLES_LEN];

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof DATA_TYPE)) return false;
            DATA_TYPE dataType = (DATA_TYPE) o;
            return Arrays.equals(array, dataType.array);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(array);
        }
    }

    public static class TIMESTAMP implements Serializable {
        public byte[] array = new byte[8];
    }

    public static long bytesToLong(byte[] b) {
        int doubleSize = 8;
        long l = 0;
        for (int i = 0; i < doubleSize; i++) {
            // 如果不强制转换为long，那么默认会当作int，导致最高32位丢失
            l |= ((long) b[i] << (8 * i)) & (0xFFL << (8 * i));
        }
        return l;
    }

    @SuppressWarnings("unchecked")
    public static <T extends Serializable> T clone(T obj) {
        T cloneObj = null;
        //写入字节流
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream obs = new ObjectOutputStream(out);
            obs.writeObject(obj);
            obs.close();

            //分配内存，写入原始对象，生成新对象
            ByteArrayInputStream ios = new ByteArrayInputStream(out.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(ios);
            //返回生成的新对象
            cloneObj = (T) ois.readObject();
            ois.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return cloneObj;
    }
    static public int ByteToInt(byte[] byteArray)
    {
        return ((byteArray[0] & 0xFF) << 24) |
                ((byteArray[1] & 0xFF) << 16) |
                ((byteArray[2] & 0xFF) << 8)  |
                ((byteArray[3] & 0xFF));
    }
    public static byte[] intToByteArray(int integer) {
        byte[] byteArray = new byte[4]; // 创建一个4字节的数组
        byteArray[0] = (byte) (integer >> 24);
        byteArray[1] = (byte) (integer >> 16);
        byteArray[2] = (byte) (integer >> 8);
        byteArray[3] = (byte) integer;
        return byteArray;
    }
    public static Pair<ArrayList<Integer>, ArrayList<TIMESTAMP>> read_data(String PATH, Integer length) throws IOException {
        ArrayList<Integer> items = new ArrayList<Integer>();
        DATA_TYPE it = new DATA_TYPE();
        ArrayList<TIMESTAMP> timestamps = new ArrayList<TIMESTAMP>();
        TIMESTAMP timestamp = new TIMESTAMP();
        File file = new File(PATH);

        FileInputStream fis = new FileInputStream(file);
        int cnt = 0;
        while (fis.read(it.array, 0, it.array.length) != -1 && fis.read(timestamp.array, 0, timestamp.array.length) != -1 && (cnt < length || length == -1)) {
            cnt++;
            items.add(ByteToInt(clone(it).array));
            timestamps.add(clone(timestamp));
        }
        fis.close();
        double end = Double.longBitsToDouble(bytesToLong(timestamps.get(cnt - 1).array)), start = Double.longBitsToDouble(bytesToLong(timestamps.get(0).array));
        System.out.println("duration:" + (end - start) + "s");
        return new Pair<>(items, timestamps);
    }
}
