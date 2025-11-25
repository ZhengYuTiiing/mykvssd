package com.ssd;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.security.SecureRandom;
import java.util.*;

/**
 * KVGeneratorString：确保读操作的key全部是之前写过的key
 */
public class NewGenerator {

    public static void main(String[] args) throws Exception {
        // 默认配置（与原逻辑一致）
        String output = "kv_workload.csv";
        long ops = 100000;
        double readRatio = 0.8;
        int numKeys = 100000;
        String keySizeMode = "fixed";
        int keySize = 20;
        int keySizeMin = 10, keySizeMax = 40;
        String valSizeMode = "fixed";
        int valSize = 1000;
        int valSizeMin = 100, valSizeMax = 2000;
        String keyDist = "uniform";
        double zipfS = 0.99;
        long seed = System.currentTimeMillis();

        // 解析参数（与原逻辑一致）
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--output": output = args[++i]; break;
                case "--ops": ops = Long.parseLong(args[++i]); break;
                case "--readRatio": readRatio = Double.parseDouble(args[++i]); break;
                case "--numKeys": numKeys = Integer.parseInt(args[++i]); break;
                case "--keySizeMode": keySizeMode = args[++i]; break;
                case "--keySize": keySize = Integer.parseInt(args[++i]); break;
                case "--keySizeMin": keySizeMin = Integer.parseInt(args[++i]); break;
                case "--keySizeMax": keySizeMax = Integer.parseInt(args[++i]); break;
                case "--valSizeMode": valSizeMode = args[++i]; break;
                case "--valSize": valSize = Integer.parseInt(args[++i]); break;
                case "--valSizeMin": valSizeMin = Integer.parseInt(args[++i]); break;
                case "--valSizeMax": valSizeMax = Integer.parseInt(args[++i]); break;
                case "--keyDist": keyDist = args[++i]; break;
                case "--zipfS": zipfS = Double.parseDouble(args[++i]); break;
                case "--seed": seed = Long.parseLong(args[++i]); break;
                default:
                    System.err.println("Unknown arg: " + args[i]);
                    System.exit(1);
            }
        }

        // 参数校验（与原逻辑一致）
        if (keySizeMode.equals("range") && keySizeMin > keySizeMax) {
            throw new IllegalArgumentException("keySizeMin > keySizeMax");
        }
        if (valSizeMode.equals("range") && valSizeMin > valSizeMax) {
            throw new IllegalArgumentException("valSizeMin > valSizeMax");
        }
        if (!(keyDist.equals("uniform") || keyDist.equals("zipf"))) {
            throw new IllegalArgumentException("keyDist must be uniform or zipf");
        }

        // 打印配置信息（增加说明）
        System.out.printf("Generating %d ops -> %s (readRatio=%.2f, numKeys=%d)\n",
                ops, output, readRatio, numKeys);
        System.out.printf("Key size mode=%s, val size mode=%s, keyDist=%s, seed=%d\n",
                keySizeMode, valSizeMode, keyDist, seed);
        System.out.println("Note: 读操作的key全部来自之前的写操作");

        Random rnd = new Random(seed);
        SecureRandom sec = new SecureRandom();

        ZipfGenerator zipf = null;
        if (keyDist.equals("zipf")) {
            zipf = new ZipfGenerator(numKeys, zipfS, rnd);
        }

        // 核心新增：记录所有已写入的key（用于读操作）
        Set<String> writtenKeys = new HashSet<>();
        // 为了高效随机读取，同步维护一个List（HashSet随机访问效率低）
        List<String> writtenKeysList = new ArrayList<>();

        try (BufferedWriter w = new BufferedWriter(new FileWriter(output))) {
            w.write("op,key_str,value_str\n");

            for (long i = 0; i < ops; i++) {
                boolean isRead;
                // 关键逻辑：若尚无写入的key，强制转为写操作
                if (writtenKeys.isEmpty()) {
                    isRead = false; // 必须先写，否则无key可读
                } else {
                    // 按比例生成读写操作（但读操作只能从已写key中选）
                    isRead = rnd.nextDouble() < readRatio;
                }

                String keyStr;
                if (isRead) {
                    // 读操作：从已写入的key中随机选择
                    int idx = rnd.nextInt(writtenKeysList.size());
                    keyStr = writtenKeysList.get(idx);
                    w.write("R," + keyStr + ",\n");
                } else {
                    // 写操作：生成新key并记录
                    int keyId;
                    if (keyDist.equals("uniform")) {
                        keyId = rnd.nextInt(numKeys);
                    } else {
                        keyId = zipf.next() - 1;
                    }

                    int curKeySize = (int) determineSize(keySizeMode, keySize, keySizeMin, keySizeMax, rnd);
                    keyStr = keyIdToKeyString(keyId, curKeySize, rnd);

                    // 记录已写入的key（去重，避免重复添加）
                    if (!writtenKeys.contains(keyStr)) {
                        writtenKeys.add(keyStr);
                        writtenKeysList.add(keyStr);
                    }

                    // 生成value并写入
                    int curValSize = (int) determineSize(valSizeMode, valSize, valSizeMin, valSizeMax, rnd);
                    String valStr = generateRandomString(curValSize, sec);
                    w.write("W," + keyStr + "," + valStr + "\n");
                }

                // 定期刷新缓冲区
                if ((i & 0xFFFFF) == 0 && i > 0) {
                    w.flush();
                }
            }
        }

        System.out.println("Done. Output: " + output);
        System.out.printf("共生成 %d 个唯一写key，%d 个操作\n", writtenKeys.size(), ops);
    }

    // 以下方法与原逻辑一致，无需修改
    private static long determineSize(String mode, int fixed, int min, int max, Random rnd) {
        switch (mode) {
            case "fixed": return fixed;
            case "range": return min + rnd.nextInt(max - min + 1);
            default: throw new IllegalArgumentException("unknown size mode: " + mode);
        }
    }

    private static String keyIdToKeyString(int keyId, int wantSize, Random rnd) {
        String base = "key_" + keyId;
        StringBuilder sb = new StringBuilder(wantSize);
        for (int i = 0; i < base.length() && i < wantSize; i++) {
            sb.append(base.charAt(i));
        }
        if (sb.length() < wantSize) {
            String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            long fixedSeed = keyId + (long) wantSize * 1000000;
            Random fixedRnd = new Random(fixedSeed);
            while (sb.length() < wantSize) {
                int charIndex = fixedRnd.nextInt(chars.length());
                sb.append(chars.charAt(charIndex));
            }
        }
        return sb.substring(0, wantSize);
    }

    private static String generateRandomString(int length, SecureRandom sec) {
        if (length <= 0) return "";
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()_-+=[]{}|;:.<>?";
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int charIndex = sec.nextInt(chars.length());
            sb.append(chars.charAt(charIndex));
        }
        return sb.toString();
    }

    static class ZipfGenerator {
        private final int size;
        private final double skew;
        private final double[] cdf;
        private final Random rnd;

        ZipfGenerator(int size, double skew, Random rnd) {
            if (size < 1) throw new IllegalArgumentException("size>=1");
            this.size = size;
            this.skew = skew;
            this.rnd = rnd;
            this.cdf = new double[size];
            double sum = 0.0;
            for (int i = 1; i <= size; i++) {
                sum += 1.0 / Math.pow(i, skew);
            }
            double acc = 0.0;
            for (int i = 1; i <= size; i++) {
                acc += (1.0 / Math.pow(i, skew)) / sum;
                cdf[i-1] = acc;
            }
            cdf[size-1] = 1.0;
        }

        int next() {
            double u = rnd.nextDouble();
            int lo = 0, hi = size - 1;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (u <= cdf[mid]) hi = mid;
                else lo = mid + 1;
            }
            return lo + 1;
        }
    }
}