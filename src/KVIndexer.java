import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KVIndexer {
    // 配置参数
    private static final int PAGE_SIZE = 32 * 1024; // 32KB
    private static final int MAX_ERROR = 3 * PAGE_SIZE; // 最大允许误差：3个页（96KB）
    private static final Pattern USER_PATTERN = Pattern.compile("user(\\d+)");
    private static final String KEY_ADDR_FILE = "key_addr.txt";
    private static final String PAGES_DIR = "pages";
    private static final String ERROR_DETAIL_FILE = "error_details.txt";
    // 布隆过滤器参数（动态计算，这里仅设置目标误判率）
    private static final double BLOOM_FALSE_POSITIVE_RATE = 0.05; // 目标误判率：1%（可调整）

    // 物理页结构（不变）
    static class PhysicalPage {
        int pageNo;
        long pageAddr;
        int currentOffset;
        List<Pair> kvs = new ArrayList<>();
        long minKeyNum;
        long maxKeyNum;

        public PhysicalPage(int pageNo) {
            this.pageNo = pageNo;
            this.pageAddr = (long) pageNo * PAGE_SIZE;
            this.currentOffset = 0;
        }
    }

    // KV对结构（不变）
    static class Pair {
        String key;
        String value;
        long keyNum;
        long addr;

        public Pair(String key, String value) {
            this.key = key;
            this.value = value;
            this.keyNum = parseKeyNum(key);
        }

        private long parseKeyNum(String key) {
            Matcher matcher = USER_PATTERN.matcher(key);
            if (matcher.find()) {
                return Long.parseLong(matcher.group(1));
            }
            throw new IllegalArgumentException("Invalid key format: " + key);
        }
    }

    // 段结构（布隆过滤器延迟初始化）
    static class Segment {
        long minKeyNum;
        long maxKeyNum;
        double a;
        double b;
        int dataCount; // 段内KV数量（用于动态计算布隆过滤器大小）
        BloomFilter bloomFilter; // 延迟初始化，待数据量明确后创建
        // 统计量
        double sumKey;
        double sumAddr;
        double sumKeySq;
        double sumKeyAddr;

        // 缓存 keyNums，等待 finalize 时批量加入 BloomFilter
        List<Long> keyBuffer = new ArrayList<>();

        public Segment(long firstKeyNum) {
            this.minKeyNum = firstKeyNum;
            this.maxKeyNum = firstKeyNum;
            this.bloomFilter = null; // 暂不初始化，等数据量明确后创建
            this.a = 0;
            this.b = 0;
            this.dataCount = 0;
        }

        public void addData(long keyNum, long addr) {
            dataCount++;
            sumKey += keyNum;
            sumAddr += addr;
            sumKeySq += (double) keyNum * keyNum;
            sumKeyAddr += (double) keyNum * addr;
            maxKeyNum = keyNum;

            // 延迟添加：只把 keyNum 缓存起来，等 finalize 时再创建布隆过滤器并批量加入
            keyBuffer.add(keyNum);

            updateModel();
        }

        // 分段完成后调用：根据最终 dataCount 计算参数并创建 BloomFilter，然后把缓存的 keys 加入
        public void finalizeSegment() {
            if (bloomFilter != null) {
                return; // 已初始化过
            }
            if (dataCount <= 0) {
                // 空段，不创建 BloomFilter
                return;
            }

            int[] params = calculateBloomParams(dataCount);
            int m = params[0];
            int k = params[1];
            bloomFilter = new BloomFilter(m, k);
            for (long kNum : keyBuffer) {
                bloomFilter.add(kNum);
            }
            // 可释放缓存以节省内存
            keyBuffer = null;
        }

        // 计算布隆过滤器最优参数（位集大小和哈希函数数）
        private int[] calculateBloomParams(int n) {
            if (n <= 0) return new int[]{1, 1}; // 边界处理

            double p = BLOOM_FALSE_POSITIVE_RATE;
            // 计算最优位集大小（m）：m = -n * ln(p) / (ln(2))²
            int m = (int) Math.ceil(-n * Math.log(p) / (Math.log(2) * Math.log(2)));
            // 计算最优哈希函数数量（k）：k = (m/n) * ln(2)
            int k = (int) Math.round((double) m * Math.log(2) / n);

            // 确保参数合理（至少1位，1个哈希函数）
            m = Math.max(m, 1);
            k = Math.max(k, 1);
            return new int[]{m, k};
        }

        private void updateModel() {
            if (dataCount < 2) {
                a = 0;
                b = sumAddr;
                return;
            }
            double avgKey = sumKey / dataCount;
            double avgAddr = sumAddr / dataCount;
            double numerator = sumKeyAddr - dataCount * avgKey * avgAddr;
            double denominator = sumKeySq - dataCount * avgKey * avgKey;
            if (denominator == 0) {
                a = 0;
                b = avgAddr;
            } else {
                a = numerator / denominator;
                b = avgAddr - a * avgKey;
            }
        }

        public Long predictAddr(long keyNum) {
            if (bloomFilter == null || !bloomFilter.mightContain(keyNum)) {
                return null;
            }
            if (keyNum >= minKeyNum && keyNum <= maxKeyNum) {
                return (long) (a * keyNum + b);
            }
            return null;
        }
    }

    // 布隆过滤器实现（不变，仅适配动态参数）
    static class BloomFilter {
        private final BitSet bitSet;
        private final int size; // 位集大小（动态计算）
        private final int hashCount; // 哈希函数数（动态计算）

        public BloomFilter(int size, int hashCount) {
            this.size = size;
            this.hashCount = hashCount;
            this.bitSet = new BitSet(size);
        }

        public void add(long value) {
            for (int i = 0; i < hashCount; i++) {
                int index = getHashIndex(value, i);
                bitSet.set(index);
            }
        }

        public boolean mightContain(long value) {
            for (int i = 0; i < hashCount; i++) {
                int index = getHashIndex(value, i);
                if (!bitSet.get(index)) {
                    return false;
                }
            }
            return true;
        }

        private int getHashIndex(long value, int seed) {
            long hash = value ^ seed;
            hash = hash * 0x5bd1e995L;
            hash ^= hash >>> 24;
            return (int) (Math.abs(hash) % size);
        }

        // 获取大小（字节）
        public int getSizeInBytes() {
            return (size + 7) / 8; // 位转字节（向上取整）
        }

        public String getStats() {
            int setBits = bitSet.cardinality();
            double usage = (size == 0) ? 0 : (double) setBits / size * 100;
            return String.format("大小：%d字节，位集：%d位，已使用：%d位 (%.2f%%)，哈希函数数：%d",
                    getSizeInBytes(), size, setBits, usage, hashCount);
        }
    }

    public static void main(String[] args) {
        createPagesDir();

        String filePath = "../memtable.data";
        List<Pair> kvs = readKVFile(filePath);
        if (kvs.isEmpty()) {
            System.out.println("未读取到数据");
            return;
        }
        System.out.println("总KV数量：" + kvs.size());

        kvs.sort(Comparator.comparingLong(p -> p.keyNum));
        List<PhysicalPage> pages = splitIntoPages(kvs);
        System.out.println("总页数：" + pages.size());

        writeKeyAddrFile(kvs);
        writePageFiles(pages);

        List<Segment> segments = dynamicSegmentation(pages);
        printSegments(segments, pages);

        // testQueries(segments, kvs);
        testAllQueries(segments,kvs);
    }

    // 工具方法：解析key为keyNum（不变）
    private static long parseKeyNum(String key) {
        Matcher matcher = USER_PATTERN.matcher(key);
        if (matcher.find()) {
            return Long.parseLong(matcher.group(1));
        }
        throw new IllegalArgumentException("Invalid key format: " + key);
    }

    // 以下为已有方法（保持不变）
    private static void createPagesDir() {
        File dir = new File(PAGES_DIR);
        if (!dir.exists() && !dir.mkdirs()) {
            System.err.println("创建pages目录失败！");
        }
    }

    private static List<Pair> readKVFile(String filePath) {
        List<Pair> kvs = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath, StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split("\\|", 2);
                if (parts.length == 2) {
                    kvs.add(new Pair(parts[0], parts[1]));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return kvs;
    }

    private static List<PhysicalPage> splitIntoPages(List<Pair> kvs) {
        List<PhysicalPage> pages = new ArrayList<>();
        PhysicalPage currentPage = new PhysicalPage(0);

        for (Pair kv : kvs) {
            int kvSize = kv.key.getBytes(StandardCharsets.UTF_8).length
                    + kv.value.getBytes(StandardCharsets.UTF_8).length;

            if (currentPage.currentOffset + kvSize > PAGE_SIZE) {
                currentPage.minKeyNum = currentPage.kvs.get(0).keyNum;
                currentPage.maxKeyNum = currentPage.kvs.get(currentPage.kvs.size() - 1).keyNum;
                pages.add(currentPage);
                currentPage = new PhysicalPage(pages.size());
            }

            kv.addr = currentPage.pageAddr + currentPage.currentOffset;
            currentPage.kvs.add(kv);
            currentPage.currentOffset += kvSize;
        }

        if (!currentPage.kvs.isEmpty()) {
            currentPage.minKeyNum = currentPage.kvs.get(0).keyNum;
            currentPage.maxKeyNum = currentPage.kvs.get(currentPage.kvs.size() - 1).keyNum;
            pages.add(currentPage);
        }

        return pages;
    }

    private static void writeKeyAddrFile(List<Pair> kvs) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(KEY_ADDR_FILE, StandardCharsets.UTF_8))) {
            bw.write("keyNum,addr\n");
            for (Pair kv : kvs) {
                bw.write(kv.keyNum + "," + kv.addr + "\n");
            }
            System.out.println("已生成 " + KEY_ADDR_FILE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writePageFiles(List<PhysicalPage> pages) {
        for (PhysicalPage page : pages) {
            String fileName = PAGES_DIR + File.separator + page.pageNo + ".txt";
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileName, StandardCharsets.UTF_8))) {
                bw.write("===== 页" + page.pageNo + " =====\n");
                bw.write("页起始地址：" + page.pageAddr + " 字节\n");
                bw.write("页内KV数量：" + page.kvs.size() + "\n");
                bw.write("keyNum范围：[" + page.minKeyNum + ", " + page.maxKeyNum + "]\n");
                bw.write("------------------------\n");
                for (Pair kv : page.kvs) {
                    bw.write(kv.key + "|" + kv.value + "\n");
                }
                System.out.println("已生成 " + fileName);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static List<Segment> dynamicSegmentation(List<PhysicalPage> pages) {
        List<Segment> segments = new ArrayList<>();
        if (pages.isEmpty()) return segments;

        PhysicalPage firstPage = pages.get(0);
        Pair firstKv = firstPage.kvs.get(0);
        Segment currentSegment = new Segment(firstKv.keyNum);

        int consecutiveErrors = 0;

        for (PhysicalPage page : pages) {
            for (Pair kv : page.kvs) {
                Long predictedAddr = currentSegment.predictAddr(kv.keyNum);
                long error = (predictedAddr == null) ? 0 : Math.abs(predictedAddr - kv.addr);

                if (error > MAX_ERROR) {
                    consecutiveErrors++;
                    if (consecutiveErrors >= 3) {
                        // 在切换段之前 finalize 当前段
                        currentSegment.finalizeSegment();
                        segments.add(currentSegment);
                        currentSegment = new Segment(kv.keyNum);
                        consecutiveErrors = 0;
                    }
                } else {
                    consecutiveErrors = 0;
                }

                currentSegment.addData(kv.keyNum, kv.addr);
            }
        }

        // finalize 并加入最后一个段
        currentSegment.finalizeSegment();
        segments.add(currentSegment);
        return segments;
    }

    // 输出分段结果时，新增页号相关计算
    private static void printSegments(List<Segment> segments, List<PhysicalPage> pages) {
        System.out.println("\n===== 分段结果 =====");
        try (BufferedWriter errorWriter = new BufferedWriter(new FileWriter(ERROR_DETAIL_FILE, StandardCharsets.UTF_8))) {
            // 误差详情文件表头新增页号相关字段
            errorWriter.write("段号,keyNum,真实地址(real_addr),真实页号(real_ppa),预测地址(pre_addr),预测页号(pre_ppa),地址误差(addr_error),页号差值(ppa_diff)\n");

            for (int i = 0; i < segments.size(); i++) {
                Segment seg = segments.get(i);
                int segmentNo = i + 1;
                System.out.printf("段%d：\n", segmentNo);
                System.out.printf("  keyNum范围：[%d, %d]\n", seg.minKeyNum, seg.maxKeyNum);
                System.out.printf("  模型：addr = %.25f × keyNum + %.2f\n", seg.a, seg.b);
                System.out.printf("  数据量：%d条KV\n", seg.dataCount);
                System.out.printf("  布隆过滤器：%s\n", seg.bloomFilter == null ? "未初始化" : seg.bloomFilter.getStats());
                System.out.printf("  目标误判率：%.2f%%\n", BLOOM_FALSE_POSITIVE_RATE * 100);

                long maxAddrError = 0; // 最大地址误差
                int maxPpaDiff = 0;    // 最大页号差值
                for (PhysicalPage page : pages) {
                    if (page.minKeyNum >= seg.minKeyNum && page.maxKeyNum <= seg.maxKeyNum) {
                        for (Pair kv : page.kvs) {
                            Long predictedAddr = seg.predictAddr(kv.keyNum);

                            // 计算真实页号（real_ppa）：真实地址 / 页大小
                            long realPpa = kv.addr / PAGE_SIZE;

                            // 计算预测页号（pre_ppa）和相关误差（若预测地址不为空）
                            long prePpa = -1;
                            long addrError = -1;
                            int ppaDiff = -1;
                            if (predictedAddr != null) {
                                prePpa = predictedAddr / PAGE_SIZE; // 预测页号
                                addrError = Math.abs(predictedAddr - kv.addr); // 地址误差
                                ppaDiff = (int) Math.abs(prePpa - realPpa); // 页号差值（取绝对值）

                                // 更新最大误差
                                if (addrError > maxAddrError) {
                                    maxAddrError = addrError;
                                }
                                if (ppaDiff > maxPpaDiff) {
                                    maxPpaDiff = ppaDiff;
                                }
                            }

                            // 写入误差详情文件（包含页号相关信息）
                            errorWriter.write(String.format(
                                    "%d,%d,%d,%d,%s,%d,%d,%d\n",
                                    segmentNo,
                                    kv.keyNum,
                                    kv.addr,          // 真实地址
                                    realPpa,          // 真实页号
                                    (predictedAddr == null ? "布隆过滤不存在" : predictedAddr), // 预测地址
                                    prePpa,           // 预测页号（-1表示未预测）
                                    addrError,        // 地址误差（-1表示未预测）
                                    ppaDiff           // 页号差值（-1表示未预测）
                            ));
                        }
                    }
                }

                // 控制台输出最大地址误差和最大页号差值
                System.out.printf("  最大地址误差：%d字节（约%.2f页）\n", maxAddrError, (double) maxAddrError / PAGE_SIZE);
                System.out.printf("  最大页号差值：%d页\n\n", maxPpaDiff);
            }
            System.out.println("已生成 " + ERROR_DETAIL_FILE + "（包含页号相关误差详情）");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 测试查询时，补充页号相关输出
    private static void testQueries(List<Segment> segments, List<Pair> allKvs) {
        System.out.println("\n===== 查询测试 =====");

        List<String> testKeys = new ArrayList<>();
        if (allKvs.size() >= 3) {
            testKeys.add(allKvs.get(0).key);
            testKeys.add(allKvs.get(allKvs.size() / 2).key);
            testKeys.add(allKvs.get(allKvs.size() - 1).key);
        }
        testKeys.add("user999999999999999999");
        testKeys.add("user000000000000000000");

        for (String key : testKeys) {
            System.out.println("\n查询key：" + key);
            long keyNum;
            try {
                keyNum = parseKeyNum(key);
            } catch (IllegalArgumentException e) {
                System.out.println("  无效key格式");
                continue;
            }

            boolean found = false;
            // 查找该key的真实地址（用于对比）
            Long realAddr = null;
            for (Pair kv : allKvs) {
                if (kv.keyNum == keyNum) {
                    realAddr = kv.addr;
                    break;
                }
            }

            for (Segment seg : segments) {
                Long preAddr = seg.predictAddr(keyNum);
                if (preAddr != null) {
                    found = true;
                    long prePpa = preAddr / PAGE_SIZE; // 预测页号
                    System.out.printf("  在段[key范围：%d~%d]中找到\n", seg.minKeyNum, seg.maxKeyNum);
                    System.out.printf("  pre_addr: %d 字节\n", preAddr);
                    System.out.printf("  pre_ppa: %d （预测物理页号）\n", prePpa);

                    // 若存在真实地址，计算并输出页号差值
                    if (realAddr != null) {
                        long realPpa = realAddr / PAGE_SIZE; // 真实页号
                        int ppaDiff = (int) Math.abs(prePpa - realPpa); // 页号差值
                        System.out.printf("  真实页号(real_ppa)：%d\n", realPpa);
                        System.out.printf("  页号差值(ppa_diff)：%d页\n", ppaDiff);
                    }
                    break;
                }
            }

            if (!found) {
                System.out.println("  未找到该key（所有段的布隆过滤器均排除）");
            }
        }
    }

    private static void testAllQueries(List<Segment> segments, List<Pair> allKvs) {
        System.out.println("\n===== 全量查询测试 =====");
        int total = allKvs.size();
        int mismatchCount = 0;
        int unFoundCount = 0;
        List<String> errorLogs = new ArrayList<>(); // 缓存误差记录，避免频繁IO

        // 记录表头
        errorLogs.add("keyNum,真实地址(real_addr),真实页号(real_ppa),预测地址(pre_addr),预测页号(pre_ppa),页号差值(ppa_diff)");

        for (Pair kv : allKvs) {
            long keyNum = kv.keyNum;
            long realAddr = kv.addr;
            long realPpa = realAddr / PAGE_SIZE;
            Long preAddr = null;
            long prePpa = -1;

            // 遍历所有段查找预测地址
            for (Segment seg : segments) {
                preAddr = seg.predictAddr(keyNum);
                if (preAddr != null) {
                    prePpa = preAddr / PAGE_SIZE;
                    break;
                }
            }

            // 统计未找到的情况
            if (preAddr == null) {
                unFoundCount++;
                continue;
            }

            // 仅记录页号不一致的情况
            int ppaDiff = (int) Math.abs(prePpa - realPpa);
            if (ppaDiff != 0) {
                mismatchCount++;
                errorLogs.add(String.format(
                        "%d,%d,%d,%d,%d,%d",
                        keyNum,
                        realAddr,
                        realPpa,
                        preAddr,
                        prePpa,
                        ppaDiff
                ));
            }
        }

        // 输出统计结果
        System.out.printf("总测试数据量：%d条\n", total);
        System.out.printf("布隆过滤器未命中：%d条 (%.2f%%)\n",
                unFoundCount, (double) unFoundCount / total * 100);
        System.out.printf("页号预测不一致：%d条 (%.2f%%)\n",
                mismatchCount, (double) mismatchCount / total * 100);

        // 写入误差详情到文件
        try (BufferedWriter bw = new BufferedWriter(new FileWriter("prediction_mismatch.txt", StandardCharsets.UTF_8))) {
            for (String line : errorLogs) {
                bw.write(line + "\n");
            }
            System.out.println("页号预测不一致的记录已保存至 prediction_mismatch.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
