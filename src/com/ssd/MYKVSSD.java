package com.ssd;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;




public class MYKVSSD {
    private static final Pattern USER_PATTERN = Pattern.compile("user(\\d+)");
    // 在类的成员变量区域添加以下代码
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final List<Pair<String, String>> memtable;
    private final Queue<List<Pair<String, String>>> immutableMemtables;
    private final Map<Integer,List<SSTable>> lsmLevels;
    private long nextSstId;
    private final List<Long> levelCapacities;
    private final long totalBlocks;
    private final Map<Long, PhysicalBlock> physicalBlocks;
    private final Queue<Long> freeBlocks;
    private int nextPageNo;
    private int nextModelId;
    private List<SSTable> sstables;
    private Stats stats;
    private KeyRangeComparator keyrangeComparator=new KeyRangeComparator();

    // ==================== 构造函数与初始化（含持久化加载）====================
    public MYKVSSD() {
        this(15L * 1024 * 1024 ); // 默认 15GB 容量
    }
    public MYKVSSD(long totalCapacity) {
        // 初始化内存结构
        this.memtable = Collections.synchronizedList(new ArrayList<>());
        this.immutableMemtables = new LinkedBlockingQueue<>();
        this.lsmLevels = new ConcurrentHashMap<>();
        this.nextSstId = 1;
        this.levelCapacities = new ArrayList<>();
        this.physicalBlocks = new ConcurrentHashMap<>();
        this.freeBlocks = new LinkedList<>();
        this.nextPageNo = 0;
        this.nextModelId = 1;
        this.stats = new Stats();


        for (int i = 0; i < 10; i++) { // 初始化 LSM 层级容量
            levelCapacities.add((long) (Constants.MAX_MEMTABLE_SIZE * Math.pow(Constants.LEVEL_RATIO, i)));
        }
        // 初始化物理块（优先从磁盘加载，无则新建）
        this.totalBlocks = totalCapacity / Constants.BLOCK_SIZE;
        
        // 从磁盘加载持久化数据（SSTable、block、page键范围树）
        //TODO: 从磁盘加载SSTable、block、page键范围;memtable,lsmtree,physicalBlocks都好了
         loadPersistedData();
         initPhysicalBlocksFromDisk( totalBlocks);

//        System.out.println("KVSSD_persistence initialized: " +
//                "totalBlocks=" + totalBlocks + ", " +
//                "loadedSSTCount=" + getTotalSSTCount() + ", " +
//                "freeBlocks=" + freeBlocks.size());
    }
    private void loadLsmLevelsFromDisk() throws IOException {
        File levelsFile = new File(Constants.LSM_LEVELS_FILE);
        if (!levelsFile.exists()) {
            System.out.println("LSM levels file not found, init empty"+Constants.LSM_LEVELS_FILE);
            return;
        }

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(levelsFile), StandardCharsets.UTF_8))) {

            lsmLevels.clear(); // 清空现有层级
            String line;
            boolean parsingEntries = false;

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                if (line.equals("----------------------------------------------------------")) {
                    parsingEntries = true;
                    continue;
                } else if (line.equals("==========================================================")) {
                    parsingEntries = false;
                    break;
                } else if (parsingEntries) {
                    // 解析格式：LEVEL|SST_ID1,SST_ID2,...
                    String[] parts = line.split("\\|", 2);
                    if (parts.length == 2) {
                        try {
                            int level = Integer.parseInt(parts[0]);
                            String[] sstIdStrs = parts[1].split(",");
                            System.out.println("Loaded SST IDs: " + line);
                            for (String sstIdStr : sstIdStrs) {
                                System.out.println("SST ID: " + sstIdStr);
                            }
                            // 为当前层级创建SSTable列表
                            List<SSTable> levelSsts = new ArrayList<>();
                            for (String sstIdStr : sstIdStrs) {
                                // 3. 关键修复1：将SST ID字符串转为长整型（SST ID通常用长整数标识）
                                long sstId = Long.parseLong(sstIdStr);

                                // 4. 关键修复2：根据SST ID构造对应的sstFile（格式：sst_{id}.sst）
                                // 先拼接文件名，再结合SST存储目录得到完整文件对象
                                String sstFileName = String.format("sst_%d.sst", sstId);
                                File sstFile = new File(Constants.SST_DIR, sstFileName); // 依赖Constants.SST_DIR（SST存储目录）

                                SSTable sst = loadSSTableFromFile(sstId, sstFile);
                                if (sst != null) {
                                    sst.level = level; // 确保SST对象的层级与解析结果一致
                                    levelSsts.add(sst);

                                } else {
                                    System.err.printf("Failed to load SST (ID=%d, Level=%d) - File not found or invalid%n", sstId, level);
                                }
                            }
                            // 将该层级添加到lsmLevels
                            lsmLevels.put(level, levelSsts);
                        } catch (NumberFormatException e) {
                            System.err.println("Invalid format in LSM levels file: " + line);
                        }
                    }
                }
            }

            System.out.println("LSM levels loaded: " + lsmLevels.size() + " levels");
            // 打印每个层级的SSTable数量（验证加载结果）
            for (Map.Entry<Integer, List<SSTable>> entry : lsmLevels.entrySet()) {
                System.out.println("  Level " + entry.getKey() + ": " + entry.getValue().size() + " SSTables");
            }
        }
    }
    /**
     * 将LSM层级结构（level -> SSTable列表）持久化到磁盘（明文格式）
     */
    private void saveLsmLevelsToDisk() throws IOException {

        Files.createDirectories(Paths.get(Constants.PERSIST_DIR));
        File levelsFile = new File(Constants.LSM_LEVELS_FILE);
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(levelsFile), StandardCharsets.UTF_8))) {

            writer.write("===================== LSM_LEVELS =====================");
            writer.newLine();
            writer.write("LEVEL_COUNT=" + lsmLevels.size());
            writer.newLine();
            writer.write("CREATION_TIME=" + dateFormat.format(new Date()));
            writer.newLine();
            writer.write("FORMAT: LEVEL|SST_ID1,SST_ID2,..."); // 格式：层级|该层级包含的SST ID列表
            writer.newLine();
            writer.write("----------------------------------------------------------");
            writer.newLine();

            // 按层级升序写入（确保加载时顺序一致）
            List<Integer> sortedLevels = new ArrayList<>(lsmLevels.keySet());
            Collections.sort(sortedLevels);

            for (int level : sortedLevels) {
                List<SSTable> ssts = lsmLevels.get(level);
                // 拼接该层级所有SST的ID（用逗号分隔）
                String sstIds = ssts.stream()
                        .map(sst -> String.valueOf(sst.sstId))
                        .collect(Collectors.joining(","));
                // 写入格式：LEVEL|SST_ID1,SST_ID2,...
                writer.write(level + "|" + sstIds);
                writer.newLine();
            }

            writer.write("==========================================================");
            writer.newLine();
        }
    }
    private void saveSSTableToFile(SSTable sst) throws IOException {
        if (sst == null) return;

        // 1. 创建 SST 目录
        Files.createDirectories(Paths.get(Constants.SST_DIR));

        // 2. 定义 SST 明文文件路径（单文件包含所有信息）
        String sstFilePath = Constants.SST_DIR + "sst_" + sst.sstId + ".sst";
        File sstFile = new File(sstFilePath);

        // 3. 使用 BufferedWriter 写入纯明文内容
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(sstFile), StandardCharsets.UTF_8))) {

            // -------------------------- SST Header --------------------------
            writer.write("==========================================================================");
            writer.newLine();
            writer.write("                        SSTable (Sorted String Table)");
            writer.newLine();
            writer.write("==========================================================================");
            writer.newLine();
            writer.write("SST_ID=" + sst.sstId);
            writer.newLine();
            writer.write("LEVEL=" + sst.level);
            writer.newLine();
            writer.write("KEY_MIN=" + sst.keyMin);
            writer.newLine();
            writer.write("KEY_MAX=" + sst.keyMax);
            writer.newLine();
            writer.write("MODEL_ID=" + sst.modelId);
            writer.newLine();
            writer.write("CREATION_TIME=" + dateFormat.format(new Date()));
            writer.newLine();
            writer.write("==========================================================================");
            writer.newLine();
            writer.newLine();

            // -------------------------- SST Footer --------------------------
            writer.write("==========================================================================");
            writer.newLine();
            writer.write("SSTable file ends here. All data is stored in plain text format.");
            writer.newLine();
        }
    }
    /**
     * 从磁盘加载 SSTable（适配明文格式）
     */
    private SSTable loadSSTableFromFile(long sstId, File sstFile) throws IOException {
        if (!sstFile.exists()) {
            System.err.println("SST file missing: " + sstFile.getAbsolutePath());
            return null;
        }
        System.out.println("Loading SSTable from file: " + sstFile.getAbsolutePath());
        // 初始化SST基本字段
        SSTable sst = new SSTable(sstId, 0); // 层级后续从文件读取
        sst.level = 0;
        sst.model_prt = new ArrayList<>();
        sst.modelId = 0;
        sst.keyMax = "";
        sst.keyMin = "";
        List<PhysicalPage> kvPages = new ArrayList<>();
        // 读取文件内容
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(sstFile), StandardCharsets.UTF_8))) {

            String line;
            // 解析状态标记
            boolean parsingHeader = true;
            boolean parsingMetadataPage = false;
            boolean parsingKvPages = false;
            boolean parsingKvPairs = false;
            PhysicalPage currentKvPage = null;
            int currentKvPageIndex = 0;

            while ((line = reader.readLine()) != null) {
                line = line.trim(); // 去除前后空格
                if (line.isEmpty()) continue; // 跳过空行

                // -------------------------- 解析头部 --------------------------
                if (parsingHeader) {
                    if (line.startsWith("LEVEL=")) {
                        sst.level = Integer.parseInt(line.substring("LEVEL=".length()));
                    } else if (line.startsWith("SST_ID=")) {
                        sst.sstId = Long.parseLong(line.substring("SST_ID=".length()));
                    } else if (line.startsWith("KEY_MIN=")) {
                        sst.keyMin = line.substring("KEY_MIN=".length());
                    } else if (line.startsWith("KEY_MAX=")) {
                        sst.keyMax = line.substring("KEY_MAX=".length());
                    }  else if (line.startsWith("MODEL_ID=")) {
                        sst.modelId = Integer.parseInt(line.substring("MODEL_ID=".length()));
                    }
                    else if (line.equals("-------------------------- METADATA_PAGE --------------------------")) {
                        parsingHeader = false;
                        parsingMetadataPage = true;
                    }
                    continue;
                }
            }
        }
        //model_prt
        Path modelPath = Paths.get(Constants.MODEL_DIR, "model_" + sst.modelId + ".mdl");

        sst.model_prt = loadModelFromFile(modelPath);
        for (Segment segment : sst.model_prt) {
            System.out.println("segment = " + segment.a + " segment.b = " + segment.b);
        }
        return sst;
    }
    /**
     * 从文件加载单个模型
     */
    private List<Segment> loadModelFromFile(Path modelPath) {
        System.out.println("Loading model from file: " + modelPath);
        List<Segment> segments = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(modelPath, StandardCharsets.UTF_8)) {
            String line;
            int modelId = -1;
            int segmentCount = 0;

            boolean inSegment = false;
            Segment currentSegment = null;
            int currentSegmentIndex = -1;

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("====")) continue;

                if (line.startsWith("MODEL_ID=")) {
                    modelId = Integer.parseInt(line.substring("MODEL_ID=".length()));
                } else if (line.startsWith("SEGMENT_COUNT=")) {
                    segmentCount = Integer.parseInt(line.substring("SEGMENT_COUNT=".length()));
                } else if (line.startsWith("-------------------------- SEGMENT ")) {
                    inSegment = true;
                    currentSegmentIndex++;
                    currentSegment = new Segment(0); // 临时初始化
                    segments.add(currentSegment);
                } else if (inSegment && line.startsWith("MIN_KEY_NUM=")) {
                    currentSegment.minKeyNum = Long.parseLong(line.substring("MIN_KEY_NUM=".length()));
                } else if (inSegment && line.startsWith("MAX_KEY_NUM=")) {
                    currentSegment.maxKeyNum = Long.parseLong(line.substring("MAX_KEY_NUM=".length()));
                } else if (inSegment && line.startsWith("A=")) {
                    // 使用BigDecimal解析高精度数值，然后转换为double
                    String aStr = line.substring("A=".length());
                    // 确保解析时保持高精度
                    BigDecimal aBigDecimal = new BigDecimal(aStr);
                    currentSegment.a = aBigDecimal.doubleValue();
                    System.out.println("Loaded A value: " + aStr + " as double: " + currentSegment.a);
                } else if (inSegment && line.startsWith("B=")) {
                    // 使用BigDecimal解析高精度数值，然后转换为double
                    String bStr = line.substring("B=".length());
                    // 确保解析时保持高精度
                    BigDecimal bBigDecimal = new BigDecimal(bStr);
                    currentSegment.b = bBigDecimal.doubleValue();
                    System.out.println("Loaded B value: " + bStr + " as double: " + currentSegment.b);
                } else if (inSegment && line.startsWith("DATA_COUNT=")) {
                    currentSegment.dataCount = Integer.parseInt(line.substring("DATA_COUNT=".length()));
                } else if (inSegment && line.startsWith("BLOOM_FILTER_SIZE=")) {
                    // 布隆过滤器相关数据会在后面处理
                    int bloomFilterSize = Integer.parseInt(line.substring("BLOOM_FILTER_SIZE=".length()));
                    line = reader.readLine().trim();
                    int bloomFilterHashCount = Integer.parseInt(line.substring("BLOOM_FILTER_HASH_COUNT=".length()));

                    // 创建布隆过滤器
                    currentSegment.bloomFilter = new BloomFilter(bloomFilterSize, bloomFilterHashCount);

                    // 读取布隆过滤器数据
                    line = reader.readLine().trim();
                    if (line.startsWith("BLOOM_FILTER_DATA=")) {
                        String bloomData = line.substring("BLOOM_FILTER_DATA=".length());
                        for (int i = 0; i < Math.min(bloomData.length(), bloomFilterSize); i++) {
                            if (bloomData.charAt(i) == '1') {
                                currentSegment.bloomFilter.bitSet.set(i);
                            }
                        }
                    }
                }
            }

        } catch (IOException | NumberFormatException e) {
            System.err.println("Failed to load model from file " + modelPath + ": " + e.getMessage());
            return new ArrayList<>(); // 发生错误时返回空列表而不是null
        }
        return segments; // 正确返回segments列表
    }
    /**
     * 将所有模型数据持久化到磁盘
     */
    private void saveModelsToDisk() throws IOException {
        Files.createDirectories(Paths.get(Constants.MODEL_DIR));

        // 遍历所有SSTable，保存其模型数据
        for (Map.Entry<Integer, List<SSTable>> entry : lsmLevels.entrySet()) {
            for (SSTable sst : entry.getValue()) {
                if (sst.model_prt != null && !sst.model_prt.isEmpty()) {
                    saveModelToFile(sst.modelId, sst.model_prt);
                }
            }
        }

        System.out.println("Models persisted to disk");
    }
    /**
     * 保存单个模型到文件
     */
    private void saveModelToFile(int modelId, List<Segment> segments) throws IOException {
        // 确保模型目录存在
        Files.createDirectories(Paths.get(Constants.MODEL_DIR));

        // 构造模型文件路径
        String modelFilePath = Constants.MODEL_DIR + "model_" + modelId + ".mdl";
        File modelFile = new File(modelFilePath);

        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(modelFile), StandardCharsets.UTF_8))) {

            // 写入文件头部
            writer.write("==========================================================================");
            writer.newLine();
            writer.write("                        Model Data");
            writer.newLine();
            writer.write("==========================================================================");
            writer.newLine();
            writer.write("MODEL_ID=" + modelId);
            writer.newLine();
            writer.write("SEGMENT_COUNT=" + segments.size());
            writer.newLine();
            writer.write("CREATION_TIME=" + dateFormat.format(new Date()));
            writer.newLine();
            writer.write("==========================================================================");
            writer.newLine();
            writer.newLine();

            // 保存每个段的数据
            for (int i = 0; i < segments.size(); i++) {
                Segment segment = segments.get(i);
                writer.write("-------------------------- SEGMENT " + i + " --------------------------");
                writer.newLine();
                writer.write("MIN_KEY_NUM=" + segment.minKeyNum);
                writer.newLine();
                writer.write("MAX_KEY_NUM=" + segment.maxKeyNum);
                writer.newLine();
                BigDecimal aFormatted = new BigDecimal(segment.a).setScale(25, RoundingMode.HALF_UP);
                BigDecimal bFormatted = new BigDecimal(segment.b).setScale(25, RoundingMode.HALF_UP);
                writer.write("A=" + aFormatted.toPlainString());
                writer.newLine();
                writer.write("B=" + bFormatted.toPlainString());
                writer.newLine();
                writer.write("DATA_COUNT=" + segment.dataCount);
                writer.newLine();

                // 保存布隆过滤器数据
                if (segment.bloomFilter != null) {
                    writer.write("BLOOM_FILTER_SIZE=" + segment.bloomFilter.size);
                    writer.newLine();
                    writer.write("BLOOM_FILTER_HASH_COUNT=" + segment.bloomFilter.hashCount);
                    writer.newLine();
                    // 保存布隆过滤器的位集状态
                    writer.write("BLOOM_FILTER_DATA=");
                    for (int j = 0; j < segment.bloomFilter.size; j++) {
                        writer.write(segment.bloomFilter.bitSet.get(j) ? "1" : "0");
                    }
                    writer.newLine();
                }
                writer.newLine();
            }

            // 写入文件尾部
            writer.write("==========================================================================");
            writer.newLine();
            writer.write("Model file ends here.");
            writer.newLine();
        }
    }
    private void loadPersistedData() {
        System.out.println("Loading persisted data...");
        try {
            loadLsmLevelsFromDisk();
            loadMemtableFromDisk();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load persisted data: " + e.getMessage());
        }

    }
    private void loadMemtableFromDisk() {
        File memFile = new File(Constants.MEMTABLE_FILE);
        if (!memFile.exists()) {
            System.out.println("No persisted memtable found.");
            return;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(memFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|", 2);
                if (parts.length == 2) {
                    memtable.add(new Pair<>(parts[0], parts[1]));
                }
            }
            System.out.println("Memtable restored from disk: " + memtable.size() + " entries");
        } catch (IOException e) {
            System.err.println("Failed to load memtable: " + e.getMessage());
        }
    }

    public void put(String key, String value) {
        int currentKvSize = calculateKvSize(new Pair<>(key, value));
        // 先检查 Memtable 剩余空间是否足够容纳当前 KV，不足则先刷盘
        synchronized (memtable) {
            int currentMemSize = memtable.stream()
                    .mapToInt(this::calculateKvSize)
                    .sum();

            // 若“当前大小 + 新KV大小”超出上限，先触发刷盘清空 Memtable
            if (currentMemSize + currentKvSize > Constants.MAX_MEMTABLE_SIZE) {
                System.out.println("Memtable 剩余空间不足，先触发刷盘（预计超出："
                        + (currentMemSize + currentKvSize - Constants.MAX_MEMTABLE_SIZE) + "字节）");
                // 手动触发刷盘（复用 checkMemtableFull 的核心逻辑，但不依赖“当前大小 >= 上限”的判断）
                flushMemtable();
            }
            // 3. 现在空间足够，写入 Memtable（原有逻辑保留，先删旧键避免重复）
            memtable.removeIf(kv -> kv.first.equals(key));
            memtable.add(new Pair<>(key, value));
        }
    }
     // 刷盘 Memtable 到 LSM 层级（原有逻辑保留，先删旧键避免重复）

    public void flushMemtable(){
        // 1. 检查 Memtable 是否为空
        if (memtable.isEmpty()) {
            System.out.println("Memtable 为空，无需刷盘");
            return;
        }
        immutableMemtables.add(new ArrayList<>(memtable));
        memtable.clear();
        // 刷盘所有不可变 Memtable（原有逻辑保留）
        while (!immutableMemtables.isEmpty()) {
            List<Pair<String, String>> immMem = immutableMemtables.poll();
            createNodeModelWritetoPage(immMem, 0);
            checkLevelCompaction(0);
        }
    }
    private int getLevelSstLimit(int level) {
        if (level < Constants.LEVEL_SST_COUNT_LIMITS.length) {
            return Constants.LEVEL_SST_COUNT_LIMITS[level];
        }
        return Constants.LEVEL_SST_COUNT_LIMITS[Constants.LEVEL_SST_COUNT_LIMITS.length - 1];
    }
    private void checkLevelCompaction(int level) {
        List<SSTable> currentLevelSsts = lsmLevels.getOrDefault(level, new ArrayList<>());
        int levelSstLimit = getLevelSstLimit(level);
        while (!currentLevelSsts.isEmpty() && currentLevelSsts.size() > levelSstLimit) {
            System.out.println("=== 触发层级 " + level + " 的 Compaction ===");
            System.out.println("当前数量：" + currentLevelSsts.size() + "，阈值：" + levelSstLimit);
            int excess=currentLevelSsts.size()-levelSstLimit;
            List<SSTable> victimSsts = new ArrayList<>();
            for (int i = 0; i < excess; i++) {
                victimSsts.add(currentLevelSsts.get(i)); // 取前excess个作为victim（超出多少选多少）
            }
            List<SSTable> targetLevelSsts = lsmLevels.get(level + 1);
            List<SSTable> overlappingSsts = new ArrayList<>();
            overlappingSsts.addAll(victimSsts);
            if(targetLevelSsts!=null){
                for (SSTable targetSst : targetLevelSsts) {
                    // 检查目标SST是否与任何一个victim重叠
                    boolean isOverlapping = false;
                    for (SSTable victim : victimSsts) {
                        Pair<String, String> victimKeyRange = new Pair<>(victim.keyMin, victim.keyMax);
                        Pair<String, String> targetKeyRange = new Pair<>(targetSst.keyMin, targetSst.keyMax);
                        if (keyrangeComparator.isOverlapping(victimKeyRange, targetKeyRange)) {
                            isOverlapping = true;
                            break;
                        }
                    }
                    if (isOverlapping) {
                        overlappingSsts.add(targetSst); // 加入所有重叠的目标SST
                    }
                }
            }
            //TODO -  Compaction
            RunCompaction(level, overlappingSsts);
            currentLevelSsts = lsmLevels.getOrDefault(level, new ArrayList<>());
            checkLevelCompaction(level + 1); // 检查下一层是否因新增超阈值
        }

    }
    private void RunCompaction(int sourceLevel, List<SSTable> overlappingSsts) {
        System.out.println("=== 触发层级 " + sourceLevel + " 的 Compaction ===");
        int targetLevel = sourceLevel + 1;
        List<Segment> uniqueSegments = new ArrayList<>();
        for (SSTable sst : overlappingSsts) {
             List<Segment> model = sst.model_prt;
             for (Segment segment : model) {
                 if (!uniqueSegments.contains(segment)) {
                    uniqueSegments.add(segment);
                 }
             }
        }
        //sort segments by key
        uniqueSegments.sort(Comparator.comparing(s -> s.minKeyNum));
        List<SSTable> newSsts = SplitIntoNonOverlappingSsts(uniqueSegments, targetLevel);

        // 2. 标记原 SSTable 为无效（业务逻辑：原 SSTable 不再参与查询）
        for(SSTable sst:overlappingSsts){
            markSSTInvalid(sst);
            deleteModelFile(sst.modelId);
        }
        List<SSTable> temp=lsmLevels.getOrDefault(targetLevel, new ArrayList<>());


        for(SSTable sst:newSsts){
            temp.add(sst);
        }
        temp.sort(Comparator.comparing(s -> s.keyMin));
        lsmLevels.put(targetLevel, temp);
        // 5. 对合并后的所有SSTable按keyRange.first（keyMin）升序排序


        // 其余持久化等逻辑保持不变.
        //TODO 存model
        try {
            for(SSTable sst:newSsts){
                saveSSTableToFile(sst);
                saveModelToFile(sst.modelId,sst.model_prt);
            }
        } catch (IOException e) {
            // System.err.printf("持久化新 SST[%d] 失败：%s%n", sst.sstId, e.getMessage());
        }
//        for(SSTable sst:newSsts){
//            System.out.println("add sstable: "+sst.sstId);
//        }
        // 更新目标层级列表到lsmLevels
        // newSsts.sort(Comparator.comparing(s -> s.keyRange.first));
        // lsmLevels.put(targetLevel, newSsts);

        //new
        // 合并新SSTable到目标层级（新老数据一起处理）
    }
    private void markSSTInvalid(SSTable sst) {
        if (sst == null) return;

        // 1. 从 LSM 层级移除
        List<SSTable> ssts = lsmLevels.get(sst.level);
        if (ssts != null) {
            ssts.remove(sst);
            if (ssts.isEmpty()) {
                lsmLevels.remove(sst.level);
            }
        }

        // 4. 持久化变更（物理块元数据、键范围树）并删除无效 SST 文件
        try {
            // 找到 SST 关联的物理块并持久化
            for (PhysicalBlock block : physicalBlocks.values()) {
                if (block.sstables.contains(sst.sstId)) {
                    block.sstables.remove(sst.sstId);
                }
            }
            // 删除无效 SST 的文件
            deleteSstFile(sst.sstId);
            // 重写 LSM 层级文件
            saveLsmLevelsToDisk();
        } catch (IOException e) {
            System.err.println("Failed to persist after marking SST invalid: " + e.getMessage());
        }
    }
        private void deleteModelFile(long modelId) {
            try {
                Files.createDirectories(Paths.get(Constants.MODEL_DIR));
                java.nio.file.Path modelPath = Paths.get(Constants.MODEL_DIR + "model_" + modelId + ".mdl");
                Files.deleteIfExists(modelPath);
            } catch (IOException e) {
                System.err.println("Failed to delete SST file sst_" + modelId + ".sst: " + e.getMessage());
            }
        }
    /**
     * 删除指定 SST 的明文文件（如果存在）。
     */
    private void deleteSstFile(long sstId) {
        try {
            Files.createDirectories(Paths.get(Constants.SST_DIR));
            java.nio.file.Path sstPath = Paths.get(Constants.SST_DIR + "sst_" + sstId + ".sst");
            Files.deleteIfExists(sstPath);
        } catch (IOException e) {
            System.err.println("Failed to delete SST file sst_" + sstId + ".sst: " + e.getMessage());
        }
    }

    private List<SSTable> SplitIntoNonOverlappingSsts(List<Segment> segments, int targetLevel) {
        List<SSTable> newSsts = new ArrayList<>();
        List<Segment> currentGroup = new LinkedList<>();
        int maxSegmentsPerSst = 1;
        int currentCount = 0;
        for (int i = 0; i < segments.size(); i++) {
            Segment segment = segments.get(i);
            currentGroup.add(segment);
            currentCount++;
            boolean needSplit = currentCount >= maxSegmentsPerSst || i == segments.size() - 1;
            if (needSplit){
                List<Segment> sharedSegments = new LinkedList<>();
                // 只有当不是最后一个元素时，才检查下一个segment
                if (i < segments.size() - 1) {
                    Segment next = segments.get(i + 1);
                    String nextMinKey = "user" + next.minKeyNum;
                    long nextMinKeyNum = next.minKeyNum;

                    for (Segment seg : currentGroup) {
                        if (seg.maxKeyNum > nextMinKeyNum) {
                            sharedSegments.add(seg);
                        }
                    }
                }

                SSTable newSst = new SSTable(nextSstId++, targetLevel);
                List<Segment> newSegments = new ArrayList<>();
                for (Segment seg : currentGroup) {
                    newSegments.add(seg);
                }
                // 4.3 计算新SSTable的键范围（论文3.C：SSTable键范围=所有KV页键范围的并集）
                newSst.keyMin = "user" + newSegments.get(0).minKeyNum;
                newSegments.sort(Comparator.comparing(s -> s.maxKeyNum));
                newSst.keyMax = "user" + newSegments.get(newSegments.size() - 1).maxKeyNum;
                 newSst.setModel_prt(newSegments);
                 newSst.modelId = nextModelId++;
                 newSsts.add(newSst);
                 currentGroup.clear();
                 //TODO 持久化到 文件
            // 8. 核心改造：持久化 SSTable 和物理块元数据
                try {
                    saveSSTableToFile(newSst);
                    saveModelToFile(newSst.modelId,newSst.model_prt);
                } catch (IOException e) {
                    System.err.println("Failed to persist SSTable/block: " + e.getMessage());
                }
                 for (Segment seg : sharedSegments) {
                    currentGroup.add(seg);
                }
                 currentCount = 0;
            }
        }
        return newSsts;
    }
    public void createNodeModelWritetoPage(List<Pair<String, String>> memtable, int level) {
        SSTable sst = new SSTable(nextSstId++, level);
        // 1. Memtable 排序 - 修改为基于数值的排序
        List<Pair<String, String>> sortedMemtable = new ArrayList<>(memtable);
        sortedMemtable.sort((p1, p2) -> {
            long num1 = parseKeyNum(p1.first);
            long num2 = parseKeyNum(p2.first);
            return Long.compare(num1, num2);
        });
        sst.keyMin = sortedMemtable.get(0).first;
        sst.keyMax = sortedMemtable.get(sortedMemtable.size() - 1).first;
        // 2. 分配物理块
        PhysicalBlock block = allocateBlock(level);
        if (block == null) {
            System.out.println("dead here.");
            return;
        }
        //System.out.println("1 block page size: " + block.pages.size());
        // 3. 拆分 KV 页
        List<PhysicalPage> kvPages = splitIntoPages(sortedMemtable, block);
        //System.out.println("page size: " + kvPages.size());
        if (kvPages == null || kvPages.isEmpty()) {
            System.err.println("Failed to split memtable into pages");
            return;
        }
       // System.out.println("2 block page size: " + block.pages.size());
       // block.pages.addAll(kvPages);
       // System.out.println("3 block page size: " + block.pages.size());
        writeKeyAddrFile(sortedMemtable);
        //3.训练出model
        List<Segment> model = dynamicSegmentation(block.pages);
        sst.modelId = nextModelId++;
        //node指向model
        sst.setModel_prt(model);
        // 7. 加入 LSM 层级
        List<SSTable> levelList = lsmLevels.computeIfAbsent(level, k -> new ArrayList<>());
        levelList.add(sst);
        // 按 min key 排序
        levelList.sort(Comparator.comparing(s -> s.keyMin));

        // 8. 核心改造：持久化 SSTable 和物理块元数据
        try {
            saveSSTableToFile(sst);
            savePhysicalBlockToFile(block);
            saveModelToFile(sst.modelId,sst.model_prt);
        } catch (IOException e) {
            System.err.println("Failed to persist SSTable/block: " + e.getMessage());
            return ;
        }
    }

    private static void writeKeyAddrFile(List<Pair<String, String>> kvs) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(Constants.KEY_ADDR_FILE, StandardCharsets.UTF_8))) {
            bw.write("keyNum,addr\n");
            for (Pair kv : kvs) {
                bw.write(kv.keyNum + "," + kv.addr + "\n");
            }
            System.out.println("已生成 " + Constants.KEY_ADDR_FILE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void savePhysicalBlockToFile(PhysicalBlock block) throws IOException {
        System.out.println("now save block: " + block.blockId+", it has "+block.pages.size()+" pages");
        if (block == null) return;

        // -------------------------- 核心步骤1：创建块专属目录 --------------------------
        // 目录路径：Constants.BLOCK_META_DIR + 块ID（如 "./block_meta/0"）
        File blockDir = new File(Constants.BLOCK_META_DIR + block.blockId);
        System.out.println("写数据到目录：" + blockDir.getAbsolutePath());
        // 若目录不存在则创建（包括父目录，避免路径不存在错误）
        if (!blockDir.exists() && !blockDir.mkdirs()) {
            throw new IOException("创建块专属目录失败：" + blockDir.getAbsolutePath());
        }

        // -------------------------- 核心步骤2：写入块基础信息到 blockdata.txt --------------------------
        File blockDataFile = new File(blockDir, "blockdata.txt");
        try (BufferedWriter blockDataWriter = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(blockDataFile), StandardCharsets.UTF_8))) {

            // 写入块基础信息（对应原需求中需要拆分的部分）
            blockDataWriter.write("===================== PHYSICAL_BLOCK =====================");
            blockDataWriter.newLine();
            blockDataWriter.write("BLOCK_ID=" + block.blockId);
            blockDataWriter.newLine();
            blockDataWriter.write("LEVEL=" + block.level);
            blockDataWriter.newLine();
            blockDataWriter.write("ALLOCATED=" + block.allocated);
            blockDataWriter.newLine();
            blockDataWriter.write("SST_COUNT=" + block.sstables.size());
            blockDataWriter.newLine();
            blockDataWriter.write("PAGE_COUNT=" + block.pages.size()); // 总页容量（如128）
            blockDataWriter.newLine();
            blockDataWriter.write("----------------------------------------------------------");
            blockDataWriter.newLine();

            // 补充关联SST列表（原逻辑中在页信息前，现在随基础信息存入blockdata.txt更合理）
            String sstList = String.join(",",
                    block.sstables.stream().map(String::valueOf).collect(Collectors.toList()));
            blockDataWriter.write("ASSOCIATED_SSTS=" + sstList);
            blockDataWriter.newLine();
            blockDataWriter.write("==========================================================");
            blockDataWriter.newLine();
        }

        // -------------------------- 核心步骤3：每个物理页单独写入编号文件（0.txt、1.txt...） --------------------------
        int pageFileIndex = 0; // 页文件编号（从0开始递增，与页的实际顺序对应）
        for (PhysicalPage page : block.pages) {
            // 只处理非空页（跳过未分配的页，避免创建空文件）
            if (page == null) {
                pageFileIndex++;
                continue;
            }

            // 页文件路径：块目录/页编号.txt（如 "./block_meta/0/0.txt"）
            File pageFile = new File(blockDir, pageFileIndex + ".txt");
            try (BufferedWriter pageWriter = new BufferedWriter(
                    new OutputStreamWriter(new FileOutputStream(pageFile), StandardCharsets.UTF_8))) {

                // 页头部信息（保留原格式，便于后续解析）
                pageWriter.write("===================== KV_PAGE_META =====================");
                pageWriter.newLine();
                pageWriter.write("PAGE_FILE_INDEX=" + pageFileIndex); // 页文件编号（与文件名对应）
                pageWriter.newLine();
                pageWriter.write("PPA=" + page.ppa); // 物理页地址
                pageWriter.newLine();
                pageWriter.write("KV_PAIR_COUNT=" + page.data.size()); // KV对数量
                pageWriter.newLine();
                pageWriter.write("----------------------------------------------------------");
                pageWriter.newLine();

                // 写入KV对（保留原转义逻辑，避免分隔符冲突）
                pageWriter.write("KV_PAIRS_START");
                pageWriter.newLine();
                for (Pair<String, String> kv : page.data) {
                    // 转义 "->" 为 "-#->"，防止解析时误分割（与原逻辑一致）
                    String escapedKey = kv.first.replace("->", "-#->");
                    String escapedValue = kv.second.replace("->", "-#->");
                    pageWriter.write(escapedKey + "->" + escapedValue);
                    pageWriter.newLine();
                }
                pageWriter.write("KV_PAIRS_END");
                pageWriter.newLine();
                // 写入相邻页的键范围信息（OOB区域）
                pageWriter.write("NEIGHBOR_KEY_RANGES_START");
                pageWriter.newLine();
                pageWriter.write("PREV_PAGE:null");
                pageWriter.newLine();
                pageWriter.write("NEXT_PAGE:null");
                pageWriter.newLine();
                pageWriter.write("NEIGHBOR_KEY_RANGES_END");
                pageWriter.newLine();

                // 页尾部标记
                pageWriter.write("==========================================================");
                pageWriter.newLine();
            }
            pageFileIndex++; // 页文件编号递增（下一个页对应下一个编号文件）
        }
        pageFileIndex = 0;
        for (PhysicalPage page : block.pages) {
            // 只处理非空页（跳过未分配的页，避免创建空文件）
            if (page == null) {
                pageFileIndex++;
                continue;
            }
            updateAdjacentPagesOOB(block, pageFileIndex,page);
            pageFileIndex++; // 页文件编号递增（下一个页对应下一个编号文件）
        }
    }

    /**
     * 更新当前页面前后相邻页面的OOB信息
     */
    private void updateAdjacentPagesOOB(PhysicalBlock block, int currentPageIndex, PhysicalPage page) throws IOException {
        // 更新前一页的OOB信息
        String MinKey = page.data.get(0).first;
        String MaxKey = page.data.get(page.data.size() - 1).first;
        if (currentPageIndex > 0) {
            // 更新当前块内的前一页这个这个page的range写入前一页的NEXT_PAGE
            File blockDir = new File(Constants.BLOCK_META_DIR + block.blockId);
            File prevPageFile = new File(blockDir, (currentPageIndex - 1) + ".txt");
            if (prevPageFile.exists()) {
                updateNextPageEntryInFile(prevPageFile, block.blockId, currentPageIndex, MinKey, MaxKey);
            }
        }else if (currentPageIndex == 0 && block.blockId > 0) {
            // 获取前一块的块ID
            long prevBlockId = block.blockId - 1;
            // 获取前一块的块目录
            File prevBlockDir = new File(Constants.BLOCK_META_DIR + prevBlockId);
            // 获取前一块的块元数据文件
            File prevPageFile = new File(prevBlockDir, (Constants.BLOCK_SIZE / Constants.PAGE_SIZE - 1) + ".txt");
            if (prevPageFile.exists()) {
                updateNextPageEntryInFile(prevPageFile, block.blockId, currentPageIndex, MinKey, MaxKey);
            }
        }
        // TODO 更新后一页的OOB信息
        if (currentPageIndex < block.pages.size() - 1) {
            // 更新当前块内的后一页，将当前页作为其PREV_PAGE信息
            File blockDir = new File(Constants.BLOCK_META_DIR + block.blockId);
            File nextPageFile = new File(blockDir, (currentPageIndex + 1) + ".txt");
            if (nextPageFile.exists()) {
                updatePrevPageEntryInFile(nextPageFile, block.blockId, currentPageIndex, MinKey, MaxKey);
            }
        } else if (currentPageIndex == block.pages.size() - 1) {
            // 更新后一个块的第一页，将当前页作为其PREV_PAGE信息
            long nextBlockId = block.blockId + 1;
            File nextBlockDir = new File(Constants.BLOCK_META_DIR + nextBlockId);
            File nextPageFile = new File(nextBlockDir, "0.txt");
            if (nextPageFile.exists()) {
                updatePrevPageEntryInFile(nextPageFile, block.blockId, currentPageIndex, MinKey, MaxKey);
            }
        }
    }
    /**
     * 更新指定文件中的PREV_PAGE条目
     */
    private void updatePrevPageEntryInFile(File pageFile, long blockId, int pageIndex, String minKey, String maxKey) throws IOException {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(pageFile), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }

        // 查找并更新PREV_PAGE条目
        boolean foundPrevPageEntry = false;
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.startsWith("PREV_PAGE:")) {;
                lines.set(i, "PREV_PAGE_" + blockId + "_" + pageIndex + "->" + minKey + "|->|" + maxKey);
                foundPrevPageEntry = true;
                break;
            }
        }

        // 如果没有找到PREV_PAGE条目，则在NEIGHBOR_KEY_RANGES_START后添加
        if (!foundPrevPageEntry) {
            for (int i = 0; i < lines.size() - 1; i++) {
                if (lines.get(i).trim().equals("NEIGHBOR_KEY_RANGES_START")) {
                    lines.add(i + 1, "PREV_PAGE_" + blockId + "_" + pageIndex + "->" + minKey + "|->|" + maxKey);
                    break;
                }
            }
        }

        // 写回文件
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(pageFile), StandardCharsets.UTF_8))) {
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
        }
    }

    /**
     * 更新指定文件中的NEXT_PAGE条目
     */
    private void updateNextPageEntryInFile(File pageFile, long blockId, int pageIndex, String minKey, String maxKey) throws IOException {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(pageFile), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }

        // 查找并更新NEXT_PAGE条目
        boolean foundNextPageEntry = false;
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.startsWith("NEXT_PAGE:")) {
                lines.set(i, "NEXT_PAGE_" + blockId + "_" + pageIndex + "->" + minKey + "|->|" + maxKey);
                foundNextPageEntry = true;
                break;
            }
        }

        // 如果没有找到NEXT_PAGE条目，则在NEIGHBOR_KEY_RANGES_START后添加
        if (!foundNextPageEntry) {
            for (int i = 0; i < lines.size() - 1; i++) {
                if (lines.get(i).trim().equals("NEIGHBOR_KEY_RANGES_START")) {
                    lines.add(i + 1, "NEXT_PAGE_" + blockId + "_" + pageIndex + "->" + minKey + "|->|" + maxKey);
                    break;
                }
            }
        }

        // 写回文件
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(pageFile), StandardCharsets.UTF_8))) {
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
        }
    }
    
    private static List<Segment> dynamicSegmentation(List<PhysicalPage> pages) {
        List<Segment> segments = new ArrayList<>();
        if (pages.isEmpty()) return segments;

        PhysicalPage firstPage = pages.get(0);
        Pair firstKv = firstPage.data.get(0);
        Segment currentSegment = new Segment(firstKv.keyNum);

        int consecutiveErrors = 0;
        int totalErrors = 0; // 新增：统计总错误数

        for (PhysicalPage page : pages) {
            for (Pair kv : page.data) {
                Long predictedAddr = currentSegment.predictAddr(kv.keyNum);
                long error = (predictedAddr == null) ? 0 : Math.abs(predictedAddr - kv.addr);

                if (error > Constants.MAX_ERROR) {
                    consecutiveErrors++;
                    totalErrors++;
                    // 修改分段条件：
                    // 1. 连续错误达到2次
                    // 2. 或者总错误数超过当前段数据量的10%
                    // 3. 或者误差特别大（超过MAX_ERROR的10倍）
                    if (consecutiveErrors >= 2 ||
                            (currentSegment.dataCount > 0 && totalErrors > currentSegment.dataCount * 0.1) ||
                            error > Constants.MAX_ERROR * 10) {

                        // 在切换段之前 finalize 当前段
                        currentSegment.finalizeSegment();
                        segments.add(currentSegment);
                        currentSegment = new Segment(kv.keyNum);
                        consecutiveErrors = 0;
                        totalErrors = 0; // 重置总错误计数
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

    /**
     * 生成物理页地址（PPA）
     */
    private String generatePPA(long blockId) {
        String ppa = blockId + "_" + nextPageNo;
        nextPageNo = (nextPageNo + 1) % (int)(Constants.BLOCK_SIZE / Constants.PAGE_SIZE);
        return ppa;
    }
    /**
     * 将排序后的 KV 对拆分成物理页
     */
    private List<PhysicalPage> splitIntoPages(List<Pair<String, String>> sortedKvs, PhysicalBlock block) {
        List<PhysicalPage> pages = new ArrayList<>();
        // 初始化当前页
        String ppa = generatePPA(block.blockId);
        PhysicalPage currentPage = new PhysicalPage(ppa);
        int pageSizeUsed = 0;
        long currentPageBaseAddr = block.blockId * Constants.BLOCK_SIZE; // 块基地址
        for (Pair<String, String> kv : sortedKvs) {
            int kvSize = kv.first.getBytes(StandardCharsets.UTF_8).length +
                    kv.second.getBytes(StandardCharsets.UTF_8).length;
            // 检查当前页是否能容纳这个 KV 对
            if (pageSizeUsed + kvSize > Constants.PAGE_SIZE) {
                // 当前页已满，需要创建新页
                if (!currentPage.data.isEmpty()) {
                   //currentPage.updateKeyRange();
                    int pageNo = Integer.parseInt(currentPage.ppa.split("_")[1]);
                    if (block.addPage(pageNo, currentPage)) {
                        pages.add(currentPage);
                        stats.totalFlashWrites += Constants.PAGE_SIZE;
                    }
                }

                // 创建新页
                ppa = generatePPA(block.blockId);
                currentPage = new PhysicalPage(ppa);
                pageSizeUsed = 0;
                // 计算新页的基地址
                int newPageNo = Integer.parseInt(ppa.split("_")[1]);
                currentPageBaseAddr = block.blockId * Constants.BLOCK_SIZE + newPageNo * Constants.PAGE_SIZE;
            }
            // 设置 KV 对的地址（块基地址 + 页内偏移）
            long kvAddr = currentPageBaseAddr + pageSizeUsed;
            kv.setAddr(kvAddr);
            // 将 KV 对添加到当前页
            currentPage.data.add(kv);
            pageSizeUsed += kvSize;
        }
        // 处理最后一页
        if (!currentPage.data.isEmpty()) {
            int pageNo = Integer.parseInt(currentPage.ppa.split("_")[1]);
            if (block.addPage(pageNo, currentPage)) {
                pages.add(currentPage);
                stats.totalFlashWrites += Constants.PAGE_SIZE;
            }
        }
        return pages;
    }


    private int calculateKvSize(Pair<String, String> kv) {
        return kv.first.getBytes(StandardCharsets.UTF_8).length
                + kv.second.getBytes(StandardCharsets.UTF_8).length;
    }

    private PhysicalBlock allocateBlock(int level) {
        // 关键优化1：检查空闲块占比，低于10%先触发GC（提前释放空间）
        if (getFreeBlockRatio() < 10) {
            System.out.println("Free block ratio < 10% (" + String.format("%.1f", getFreeBlockRatio()) + "%), trigger GC in advance");
            runGarbageCollection();
        }
        // 1. 优先查找同层级空闲块
        Iterator<Long> iterator = freeBlocks.iterator();
        while (iterator.hasNext()) {
            long blockId = iterator.next();
            PhysicalBlock block = physicalBlocks.get(blockId);
            if (block.level == level && !block.isFull()) {
                iterator.remove();
                block.allocated = true;
//                // 持久化块的分配状态变更
//                try {
//          //          savePhysicalBlockToFile(block);
//                } catch (IOException e) {
//                    System.err.println("Failed to persist block after allocate: " + e.getMessage());
//                }
                return block;
            }
        }

        // 2. 无同层级空闲块，再次检查空闲块占比（防止GC后仍无块）
        if (freeBlocks.isEmpty()) {
            System.err.println("No free blocks left! Force GC again");
            runGarbageCollection();
            // 若GC后仍无块，返回null（避免死循环）
            if (freeBlocks.isEmpty()) {
                System.err.println("KVSSD capacity exhausted!");
                return null;
            }
        }

        // 3. 分配新块并设置层级
        long blockId = freeBlocks.poll();
        PhysicalBlock block = physicalBlocks.get(blockId);
        block.level = level;
        block.allocated = true;
        // 持久化块的元数据变更
//        try {
//         //   savePhysicalBlockToFile(block);
//        } catch (IOException e) {
//            System.err.println("Failed to persist new block: " + e.getMessage());
//        }
        return block;
    }
    /**
     * 计算空闲块占总块数的比例（百分比）
     */
    private double getFreeBlockRatio() {
        // 加锁保证freeBlocks和totalBlocks的原子性读取
        synchronized (freeBlocks) {
            return (double) freeBlocks.size() / totalBlocks * 100;
        }
    }
    /**
     * 从磁盘加载物理块元数据（明文格式）
     */
    private void initPhysicalBlocksFromDisk(long totalBlocks) {
        try {
            // 创建物理块元数据目录（不存在则创建）
            Files.createDirectories(Paths.get(Constants.BLOCK_META_DIR));

            // 遍历所有可能的块 ID，加载元数据
            for (long blockId = 0; blockId < totalBlocks; blockId++) {
                File metaFile = new File(Constants.BLOCK_META_DIR + blockId + Constants.BLOCK_META_SUFFIX);
                if (metaFile.exists()) {
                    // 从明文文件加载物理块
                    PhysicalBlock block = loadPhysicalBlockFromFile(blockId, metaFile);
                    physicalBlocks.put(blockId, block);
                    // 若块未分配，加入空闲列表
                    if (!block.allocated) {
                        freeBlocks.add(blockId);
                    }
                } else {
                    // 无元数据文件，创建新块
                    PhysicalBlock newBlock = new PhysicalBlock(blockId);
                    physicalBlocks.put(blockId, newBlock);
                    freeBlocks.add(blockId);
                    // 持久化新块的初始元数据（明文格式）
                   // savePhysicalBlockToFile(newBlock);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to init physical blocks from disk: " + e.getMessage());
        }
    }

    private PhysicalBlock loadPhysicalBlockFromFile(long blockId, File metaFile) throws IOException {
        PhysicalBlock block = new PhysicalBlock(blockId);

        boolean parsingPages = false;
        PhysicalPage currentPage = null;
        int currentPageNo = -1;
        boolean parsingKvPairs = false;
        boolean parsingNeighborRanges = false;
        // 用于匹配KV_PAGE开始标识的正则（仅匹配=== KV_PAGE_数字 ===）
        Pattern pageStartPattern = Pattern.compile("=== KV_PAGE_(\\d+) ===");

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(metaFile), StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                // 解析块级属性
                if (line.startsWith("LEVEL=")) {
                    block.level = Integer.parseInt(line.substring("LEVEL=".length()));
                } else if (line.startsWith("ALLOCATED=")) {
                    block.allocated = Boolean.parseBoolean(line.substring("ALLOCATED=".length()));
                } else if (line.startsWith("ASSOCIATED_SSTS=")) {
                    String sstStr = line.substring("ASSOCIATED_SSTS=".length());
                    if (!sstStr.isEmpty()) {
                        Arrays.stream(sstStr.split(","))
                                .map(String::trim)
                                .forEach(sstId -> block.sstables.add(Long.parseLong(sstId)));
                    }
                }

                // 切换页面解析状态
                else if (line.equals("PAGES_START")) {
                    parsingPages = true;
                } else if (line.equals("PAGES_END")) {
                    parsingPages = false;
                    if (currentPage != null && currentPageNo != -1) {
                        block.addPage(currentPageNo, currentPage);
                        currentPage = null;
                        currentPageNo = -1;
                    }
                }

                // 解析页面内容
                else if (parsingPages) {
                    // 匹配页面开始标识（仅处理=== KV_PAGE_数字 ===格式）
                    Matcher startMatcher = pageStartPattern.matcher(line);
                    if (startMatcher.matches()) {
                        // 保存上一个页
                        if (currentPage != null && currentPageNo != -1) {
                            block.addPage(currentPageNo, currentPage);
                        }
                        // 提取页编号（正则分组1即为数字部分）
                        String pageNoStr = startMatcher.group(1);
                        currentPageNo = Integer.parseInt(pageNoStr) - 1; // 转为0基索引
                        currentPage = new PhysicalPage("");
                        parsingKvPairs = false;
                        continue; // 跳过后续处理，进入下一行解析
                    }

                    // 解析页属性（PPA/VALID/REF_COUNT等）
                    else if (currentPage != null && !parsingKvPairs) {
                        if (line.startsWith("PPA=")) {
                            currentPage.ppa = line.substring("PPA=".length()).trim();
                        }  else if (line.equals("KV_PAIRS_START")) {
                            parsingKvPairs = true;
                        }
                    }
                    // 解析KV对数据
                    else if (parsingKvPairs && currentPage != null) {
                        if (line.equals("KV_PAIRS_END")) {
                            parsingKvPairs = false;
                        } else {
                            String[] kvParts = line.split("->", 2);
                            if (kvParts.length == 2) {
                                currentPage.data.add(new Pair<>(kvParts[0].trim(), kvParts[1].trim()));
                            } else {
                                // System.err.printf("无效的KV对格式（块%d，页%d）：%s%n",
                                //         blockId, currentPageNo + 1, line);
                            }
                        }
                    }
                    else if (parsingNeighborRanges && currentPage != null) {
                        if (line.equals("NEIGHBOR_KEY_RANGES_END")) {
                            parsingNeighborRanges = false;
                        } else if (!line.equals("KV_PAIRS_END")) {
                            // 解析相邻页键范围数据
                            // 格式: PREV_PAGE_0->key1|->|key2 或 NEXT_PAGE_1->key3|->|key4
                            // 或格式: PREV_BLOCK_PAGE_0_127->key5|->|key6 或 NEXT_BLOCK_PAGE_4_0->key7|->|key8
                            String[] parts = line.split("->", 2);
                            if (parts.length == 2) {
                                String pageIdentifier = parts[0]; // 页标识符
                                String[] keyRangeParts = parts[1].split("\\|->\\|", 2); // 键范围
                                if (keyRangeParts.length == 2) {
                                    String minKey = keyRangeParts[0];
                                    String maxKey = keyRangeParts[1];
                                    // 可以将这些信息存储到 currentPage 的某个字段中
                                   // currentPage.neighborKeyRanges.put(pageIdentifier, new Pair<>(minKey, maxKey));
                                    System.out.println("Loaded neighbor key range: " + pageIdentifier +
                                            " -> [" + minKey + ", " + maxKey + "]");
                                }
                            }
                        }
                    }

                    // 处理页面结束标识（不提取编号，仅重置当前页状态）
                    if (line.startsWith("=== END_KV_PAGE_")) {
                        if (currentPage != null && currentPageNo != -1) {
                            block.addPage(currentPageNo, currentPage);
                            currentPage = null;
                            currentPageNo = -1;
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.printf("解析物理块%d失败：%s%n", blockId, e.getMessage());
            throw e;
        }

        return block;
    }
    public void runGarbageCollection() {

    }
//    public String get(String key) {
//       // System.out.println("1111");
//        stats.readCount++;
//        // 1. 查活跃 Memtable
//        synchronized (memtable) {
//            for (Pair<String, String> kv : memtable) {
//                if (kv.first.equals(key)) {
//                    return kv.second;
//                }
//            }
//        }
//        ///System.out.println("memtable not found 2222222");
//        // 2. 查不可变 Memtable
//        for (List<Pair<String, String>> immMem : immutableMemtables) {
//            for (Pair<String, String> kv : immMem) {
//                if (kv.first.equals(key)) {
//                    return kv.second;
//                }
//            }
//        }
//        // 3. 查 LSM 树（SSTable）
//        for (int i=0;i<lsmLevels.size();i++) {
//            List<SSTable> Ssts = lsmLevels.get(i);
//            if (Ssts == null || Ssts.isEmpty()) {
//                continue;
//            }
//           // System.out.println("3333333 search in lsm level "+i);
//            // 2. 遍历当前层级的SSTable（已按键范围起始值升序排列，利用不重叠特性优化）
//            for (SSTable sst : Ssts) {
//
//                // 2.1 目标键 < 当前SSTable的起始键：后续SSTable起始键更大，直接跳出该层级
//                // 修改SSTable键范围比较逻辑，使用数值比较
//                if (parseKeyNum(key) < parseKeyNum(sst.keyMin))
//                    break;
//
//                // 2.2 目标键 > 当前SSTable的结束键：继续检查下一个SSTable
//                if (parseKeyNum(key) > parseKeyNum(sst.keyMax)) {
//                    // System.out.println("skip this sst"+sst.sstId);
//                    continue;
//                }
//
//             //   System.out.println("hit sst"+sst.sstId);
//                // -------------------------- 核心修改1：解析Meta Page PPA，定位【Meta Block元数据文件】 --------------------------
//                // Meta Page PPA格式："块ID_页偏移"（如"0_0" → 块ID=0，页偏移忽略）
//                //  System.out.println("sst中读出来meta pag ppa ："+sst.metadataPagePpa);
//                //  System.out.println("metappa:"+sst.metadataPagePpa);
//                List<Segment> model = sst.model_prt;
//
//                for (Segment seg : model) {
//                    //System.out.println("进入seg");
//                    // 将字符串键转换为数字键进行预测
//
//                    long keyNum =parseKeyNum(key); // 简化处理，实际应该使用更好的哈希或编码方式
//                    // 使用模型预测地址
//                    Long predictedAddr = seg.predictAddr(keyNum);
//                    if (predictedAddr != null) {
//                        // 根据PPA解析块ID和页号
//                        // 将地址转换为块号和页号
//                        long blockId = predictedAddr / Constants.BLOCK_SIZE;
//                        long pageOffset = predictedAddr % Constants.BLOCK_SIZE;
//                        int pageNo = (int) (pageOffset / Constants.PAGE_SIZE);
//
//                            // 构造文件路径：BLOCK_META_DIR/blockId/pageNo.txt
//                            String pageFilePath = Constants.BLOCK_META_DIR + blockId + "/" + pageNo + ".txt";
//                            File pageFile = new File(pageFilePath);
//
//                            // 从文件中读取数据
//                            if (pageFile.exists()) {
//                                try (BufferedReader reader = new BufferedReader(new FileReader(pageFile))) {
//                                    boolean inKvPairs = false;
//                                    String line;
//
//                                    while ((line = reader.readLine()) != null) {
//                                        line = line.trim();
//                                        if (line.equals("KV_PAIRS_START")) {
//                                            inKvPairs = true;
//                                            continue;
//                                        } else if (line.equals("KV_PAIRS_END")) {
//                                            inKvPairs = false;
//                                            continue;
//                                        }
//
//                                        if (inKvPairs) {
//                                            String[] kvParts = line.split("->", 2);
//                                            if (kvParts.length == 2) {
//                                                String fileKey = kvParts[0].trim();
//                                                String fileValue = kvParts[1].trim();
//
//                                                if (fileKey.equals(key)) {
//                                                    return fileValue;
//                                                }
//                                            }
//                                        }
//                                    }
//                                } catch (IOException e) {
//                                    System.err.println("Failed to read page file: " + pageFilePath);
//                                }
//                            }
//
//                    }
//                }
//            }
//        }
//        // 4. 未找到
//        return null;
//    }
    public String get(String key) {
    stats.readCount++;
    // 1. 查活跃 Memtable
    synchronized (memtable) {
        for (Pair<String, String> kv : memtable) {
            if (kv.first.equals(key)) {
                return kv.second;
            }
        }
    }
    // 2. 查不可变 Memtable
    for (List<Pair<String, String>> immMem : immutableMemtables) {
        for (Pair<String, String> kv : immMem) {
            if (kv.first.equals(key)) {
                return kv.second;
            }
        }
    }
    // 3. 查 LSM 树（SSTable）
    for (int i=0;i<lsmLevels.size();i++) {
        List<SSTable> Ssts = lsmLevels.get(i);
        if (Ssts == null || Ssts.isEmpty()) {
            continue;
        }
        // 2. 遍历当前层级的SSTable（已按键范围起始值升序排列，利用不重叠特性优化）
        for (SSTable sst : Ssts) {

            // 2.1 目标键 < 当前SSTable的起始键：后续SSTable起始键更大，直接跳出该层级
            // 修改SSTable键范围比较逻辑，使用数值比较
//            if (parseKeyNum(key) < parseKeyNum(sst.keyMin))
//                break;

            // 2.2 目标键 > 当前SSTable的结束键：继续检查下一个SSTable
            if (parseKeyNum(key) > parseKeyNum(sst.keyMax)) {
                continue;
            }
            System.out.println("target key hit sst: " + sst.sstId);
            List<Segment> model = sst.model_prt;

            for (Segment seg : model) {
                long keyNum = parseKeyNum(key);
                // 使用模型预测地址
                Long predictedAddr = seg.predictAddr(keyNum);
                if (predictedAddr != null) {
                    // 根据PPA解析块ID和页号
                    long blockId = predictedAddr / Constants.BLOCK_SIZE;
                    long pageOffset = predictedAddr % Constants.BLOCK_SIZE;
                    int pageNo = (int) (pageOffset / Constants.PAGE_SIZE);
                    System.out.println("predicted ppa: " + blockId+"_"+pageNo);
                    // 构造文件路径：BLOCK_META_DIR/blockId/pageNo.txt
                    String pageFilePath = Constants.BLOCK_META_DIR + blockId + "/" + pageNo + ".txt";
                    File pageFile = new File(pageFilePath);
                    System.out.println("page file path: " + pageFilePath);
                    // 从文件中读取数据
                    if (pageFile.exists()) {
                        String value = readFromPageFile(pageFile, key);
                        if (value != null) {
                            return value; // 在预测页找到key
                        }
                        System.out.println("predicted page file not found,searching neighbor pages.");
                         value = readFromNeighborPages(pageFile, key, blockId, pageNo);
                        // 如果在预测页未找到，检查相邻页
                        if(value != null){
                            return value;
                        }
                    }
                }
            }
        }
    }
    // 4. 未找到
    return null;
}

/**
 * 从指定页面文件读取数据
 */
private String readFromPageFile(File pageFile, String targetKey) {
    try (BufferedReader reader = new BufferedReader(new FileReader(pageFile))) {
        boolean inKvPairs = false;
        String line;

        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.equals("KV_PAIRS_START")) {
                inKvPairs = true;
                continue;
            } else if (line.equals("KV_PAIRS_END")) {
                inKvPairs = false;
                continue;
            }

            if (inKvPairs) {
                String[] kvParts = line.split("->", 2);
                if (kvParts.length == 2) {
                    String fileKey = kvParts[0].trim();
                    String fileValue = kvParts[1].trim();

                    if (fileKey.equals(targetKey)) {
                        return fileValue;
                    }
                }
            }
        }
    } catch (IOException e) {
        System.err.println("Failed to read page file: " + pageFile.getAbsolutePath());
    }
    return null;
}

/**
 * 检查相邻页是否包含目标key
 */
private String readFromNeighborPages(File currentPageFile, String targetKey, long currentBlockId, int currentPageNo) {
    System.out.println("checking neighbor pages. pageFilePath: " + currentPageFile.getAbsolutePath());
    try (BufferedReader reader = new BufferedReader(new FileReader(currentPageFile))) {
        boolean inNeighborRanges = false;
        String line;
        String prevPageInfo = null;
        String nextPageInfo = null;

        while ((line = reader.readLine()) != null) {
            line = line.trim();

                if (line.startsWith("PREV_PAGE_")) {
                    prevPageInfo = line.substring("PREV_PAGE_".length());
                } else if (line.startsWith("NEXT_PAGE_")) {
                    nextPageInfo = line.substring("NEXT_PAGE_".length());
                }

        }

        long targetKeyNum = parseKeyNum(targetKey);

        // 检查前一页
        System.out.println("checking prev page. prevPageInfo: " + prevPageInfo);
        if (prevPageInfo != null) {
            String value = checkNeighborPage(prevPageInfo, targetKey, targetKeyNum);
            if (value != null) {
                return value;
            }
        }
        // 检查后一页
        System.out.println("checking next page. nextPageInfo: " + nextPageInfo);
        if (nextPageInfo != null) {
            String value = checkNeighborPage(nextPageInfo, targetKey, targetKeyNum);
            if (value != null) {
                return value;
            }
        }
    } catch (IOException e) {
        System.err.println("Failed to read neighbor ranges from page file: " + currentPageFile.getAbsolutePath());
    }

    return null;
}

/**
 * 检查指定相邻页是否包含目标key
 */
private String checkNeighborPage(String pageInfo, String targetKey, long targetKeyNum) {
    // 解析相邻页信息: blockId_pageNo->minKey|->|maxKey
    String[] parts = pageInfo.split("->", 2);
    if (parts.length != 2) {
        return null;
    }

    String pageIdentifier = parts[0];
    String[] keyRange = parts[1].split("\\|->\\|", 2);
    if (keyRange.length != 2) {
        return null;
    }

    String minKey = keyRange[0];
    String maxKey = keyRange[1];

    // 检查目标key是否在该页的范围内
    long minKeyNum = parseKeyNum(minKey);
    long maxKeyNum = parseKeyNum(maxKey);

    if (targetKeyNum >= minKeyNum && targetKeyNum <= maxKeyNum) {
        System.out.println("target key found in neighbor page: " + pageIdentifier);
        // 构造相邻页文件路径
        String[] pageParts = pageIdentifier.split("_");
        if (pageParts.length == 2) {
            try {
                long blockId = Long.parseLong(pageParts[0]);
                int pageNo = Integer.parseInt(pageParts[1]);

                String neighborPagePath = Constants.BLOCK_META_DIR + blockId + "/" + pageNo + ".txt";
                File neighborPageFile = new File(neighborPagePath);

                if (neighborPageFile.exists()) {
                    return readFromPageFile(neighborPageFile, targetKey);
                }
            } catch (NumberFormatException e) {
                System.err.println("Failed to parse page identifier: " + pageIdentifier);
            }
        }
    }

    return null;
}

    private long parseKeyNum(String key) {
        // 处理空键或null键的情况
        if (key == null || key.isEmpty()) {
            return 0; // 或者返回其他默认值
        }

        Matcher matcher = USER_PATTERN.matcher(key);
        if (matcher.find()) {
            try {
                return Long.parseLong(matcher.group(1));
            } catch (NumberFormatException e) {
                // 处理数字解析失败的情况
                return 0; // 或者返回其他默认值
            }
        }
        return 0;

    }

    /**
     * 关闭KVSSD并持久化所有数据到磁盘
     */
    public void shutdown() {
        try {
            // 1. 持久化Memtable
            saveMemtableToDisk();

            // 2. 刷盘所有未持久化的Memtable
            //flushMemtable();

            // 3. 持久化LSM层级结构
            saveLsmLevelsToDisk();

            // 4. 持久化所有模型数据
            saveModelsToDisk();


            System.out.println("KVSSD shutdown completed. All data persisted to disk.");
        } catch (IOException e) {
            System.err.println("Failed to persist data during shutdown: " + e.getMessage());
        }
    }
    private void saveMemtableToDisk() {
        File dir = new File(Constants.PERSIST_DIR);
        dir.mkdirs();
        File file = new File(Constants.PERSIST_DIR + "memtable.data");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            for (Pair<String, String> kv : memtable) {
                writer.write(kv.first + "|" + kv.second);
                writer.newLine();
            }
            System.out.println("Memtable persisted to " + file.getAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException("Failed to save memtable: " + e.getMessage());
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

            double p = Constants.BLOOM_FALSE_POSITIVE_RATE;
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
          //  System.out.println("Key not found in bloom filter.");
            return null;
        }
        //System.out.println("Checking keyNum: " + keyNum);
        //System.out.println("Segment minKeyNum: " + minKeyNum + ", maxKeyNum: " + maxKeyNum);
        if (keyNum >= minKeyNum && keyNum <= maxKeyNum) {
               // System.out.println("Key " + keyNum + " found in segment.");
            double result = a * keyNum + b;
            long predicted = (long) result;
//            System.out.println("Predicting address for keyNum: " + keyNum +
//                              ", a: " + a + ", b: " + b +
//                              ", result: " + result + ", predicted: " + predicted);
            return predicted;
        }
        return null;
    }

    }
    /**
     * 统计信息类
     */
    public static class Stats {
        public long writeCount; // 应用写入次数
        public long readCount; // 应用读取次数
        public long gcCount; // GC 次数
        public long compactionCount; // 压缩次数
        public double writeAmplification;// 写入放大
        public long totalFlashWrites; // 闪存总写入字节
        public long totalFlashReads; // 闪存总读取次数
        public int read0Flash; // 0次闪存访问的读取
        public int read1Flash; // 1次闪存访问的读取
        public int read2Flash; // 2次闪存访问的读取
        public int read3Flash; // 3次闪存访问的读取
        public int read4Flash; // 4次闪存访问的读取
        public int read5Flash; // 5次闪存访问的读取
        public int read6Flash; // 6次闪存访问的读取
        public int read7Flash; // 7次闪存访问的读取
        public int read8Flash; // 8次闪存访问的读取
        public int readMoreFlash; // 8次以上闪存访问的读取

        public Stats() {
            // 默认初始化所有字段为 0
            this.writeCount = 0;
            this.readCount = 0;
            this.gcCount = 0;
            this.compactionCount = 0;
            this.writeAmplification = 0.0;
            this.totalFlashWrites = 0;
            this.totalFlashReads = 0;
            this.read0Flash = 0;
            this.read1Flash = 0;
            this.read2Flash = 0;
            this.read3Flash = 0;
            this.read4Flash = 0;
            this.read5Flash = 0;
            this.read6Flash = 0;
            this.read7Flash = 0;
            this.read8Flash = 0;
            this.readMoreFlash = 0;
        }
    }
    /**
     * 物理页类（含持久化所需的元数据）
     */
    public static class PhysicalPage {
        public String ppa;                  // 物理页地址
        public List<Pair<String, String>> data; // KV 数据
        public List<Pair<String, String>> reverse_map; // 反向映射表

        public PhysicalPage(String ppa) {
            this.ppa = ppa;
            this.data = new ArrayList<>();
            this.reverse_map = new ArrayList<>();
        }
     }

    /**
     * 物理块类
     */
    public static class PhysicalBlock {
        public long blockId;               // 块 ID
        public List<PhysicalPage> pages;   // 块内页
        public int level;                  // 所属层级（-1=未分配）
        public Set<Long> sstables;         // 关联的 SST ID
        public boolean allocated;          // 分配状态

        public PhysicalBlock(long blockId) {
            this.blockId = blockId;
            this.level = -1;
            this.sstables = new HashSet<>();
            this.allocated = false;
            // 初始化页列表（4MB / 32KB = 128 个页）
            this.pages = new ArrayList<>(Collections.nCopies((int) (Constants.BLOCK_SIZE / Constants.PAGE_SIZE), null));
        }
        public boolean isFull() {
            return pages.stream().filter(Objects::nonNull).count() >= pages.size();
        }
        public boolean addPage(int pageNo, PhysicalPage page) {
            if (pageNo < 0 || pageNo >= pages.size() || pages.get(pageNo) != null) {
                return false;
            }
            pages.set(pageNo, page);
            return true;
        }

    }
    /**
     * SSTable_Node 类
     */
    public static class SSTable {
        public long sstId;                 // SST ID
        public int level;                  // 所属层级
        public int modelId;                // 模型 IDb

        public String keyMin;
        public String keyMax;
        public List<Segment> model_prt;

        public SSTable(long sstId, int level) {
            this.sstId = sstId;
            this.level = level;
        }
        public void setKeyRange(String keyMin, String keyMax) {
            this.keyMin = keyMin;
            this.keyMax = keyMax;
        }
        public void setModel_prt(List<Segment> model_prt) {
            this.model_prt = model_prt;
        }
    }

    public static class Modle {
        List<Segment> segments;
        int model_id;
    }
//    public static void main(String[] args) {
//        MYKVSSD kvssd = new MYKVSSD();
//        Runtime.getRuntime().addShutdownHook(new Thread(kvssd::shutdown));
//        kvssd.put("user2719434334763201561", "value2719434334763201561");
//        String value=kvssd.get("user2719434334763201561");
//        if(value!=null){
//            System.out.println("Read success!");
//            System.out.println("Value: "+value);
//        }else{
//            System.out.println("Read fail!");
//        }
//    }
    class KeyRangeComparator {
        // 1. 比较两个String键的大小（返回-1: a<b，0:a==b，1:a>b）
        public int compare(String a, String b) {
            return a.compareTo(b);
        }

        // 2. 判断两个键范围（Pair<String, String>）是否重叠（论文3.C核心逻辑：重叠则需合并）
        // 入参：r1、r2 - 键范围，格式为 Pair<最小键, 最大键>
        // 逻辑：仅当“r1的最大键 < r2的最小键”或“r2的最大键 < r1的最小键”时无重叠，否则重叠
        public boolean isOverlapping(Pair<String, String> r1, Pair<String, String> r2) {
            // 先获取两个范围的最小键和最大键（确保Pair的第一个元素是最小键，第二个是最大键，论文3.B）
            String r1Min = r1.first;   // r1的最小键
            String r1Max = r1.second; // r1的最大键
            String r2Min = r2.first;   // r2的最小键
            String r2Max = r2.second; // r2的最大键

            // 论文3.A重叠判断逻辑：排除“完全不重叠”的两种情况，剩余为重叠
            boolean noOverlapCase1 = compare(r1Max, r2Min) < 0; // r1的最大键 < r2的最小键
            boolean noOverlapCase2 = compare(r2Max, r1Min) < 0; // r2的最大键 < r1的最小键

            return !(noOverlapCase1 || noOverlapCase2); // 非“完全不重叠”即“重叠”
        }
    }

    private static List<Pair<String,String>> generateKVData() {
        List<Pair<String,String>> kvs = new ArrayList<>();
        Random random = new Random();

        // 估算需要生成的数据量以达到约10MB
        // 假设每条记录平均约为100字节（包括key、value和分隔符）
        int targetRecords = (10 * 1024 * 1024) / 100;

        StringBuilder valueBuilder = new StringBuilder();

        for (int i = 0; i < targetRecords; i++) {
            // 生成随机keyNum
            long keyNum = Math.abs(random.nextLong());
            String key = "user" + keyNum;

            // 生成随机value，长度在50-150字符之间
            valueBuilder.setLength(0);
            int valueLength = 50 + random.nextInt(100);
            for (int j = 0; j < valueLength; j++) {
                char c = (char) (32 + random.nextInt(95)); // ASCII 32-126 (可打印字符)
                valueBuilder.append(c);
            }

            kvs.add(new Pair(key, valueBuilder.toString()));
        }

        return kvs;
    }
    public static void main(String[] args) {
        MYKVSSD kvssd = new MYKVSSD();
//        // 测试多个key的读取操作
//        String[] testKeys = {
//                "user292713126068779000",
//                "user718011042563105175",
//                "user1498413614602006438",
//                "user6767619306422110467",
//                "user9062295983798595795",
//                "user666363271010298725",
//                "user1610487139809764312",
//                "user3393528578134494861",
//                "user9132804061154425932",
//                "user227058404529001044",
//        };
//
//        int successCount = 0;
//        for (String key : testKeys) {
//            String value = kvssd.get(key);
//            if (value != null) {
//                System.out.println("读取成功: " + key + " -> " + value);
//                successCount++;
//            } else {
//                System.out.println("读取失败: " + key);
//            }
//        }
//
//        System.out.println("读取测试完成，成功读取 " + successCount + "/" + testKeys.length + " 条数据");


        Runtime.getRuntime().addShutdownHook(new Thread(kvssd::shutdown));
        try {

            List<Pair<String,String>> kvs = generateKVData();
            for (Pair<String,String> kv : kvs) {
                kvssd.put(kv.first, kv.second);
            }


        } catch (Exception e) {
            System.err.println("测试过程中发生错误: " + e.getMessage());
            e.printStackTrace();
        }

    }


}
