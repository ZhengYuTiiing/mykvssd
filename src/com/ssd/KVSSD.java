package com.ssd;

import java.io.*;
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

/**
 * 支持持久化的 KVSSD 模拟器（基于 LSM 树），使用明文格式存储数据
 */
public class KVSSD{
    // 内存数据结构（需与磁盘同步）
    private final List<Pair<String, String>> memtable;
    private final Queue<List<Pair<String, String>>> immutableMemtables;
    private final Map<Integer, List<SSTable>> lsmLevels;
    private long nextSstId;
    private final List<Long> levelCapacities;
    private final long totalBlocks;
    private final Map<Long, PhysicalBlock> physicalBlocks;
    private final Queue<Long> freeBlocks;
    private int nextPageNo;
    private Stats stats;
    // 元数据区专用块（独立于数据区的physicalBlocks）
    private final Map<Long, PhysicalBlock> metaPhysicalBlocks;
    private final Queue<Long> metaFreeBlocks; // 元数据区空闲块队列
    // 持久化相关标记（确保刷盘时不重复操作）
    private boolean isPersisting = false;
    // 日期格式化器（统一时间格式）
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private KeyRangeComparator keyrangeComparator=new KeyRangeComparator();

    // ==================== 构造函数与初始化（含持久化加载）====================
    public KVSSD() {

        this(15L * 1024 * 1024 * 1024); // 默认 15GB 容量
    }

    public KVSSD(long totalCapacity) {
        System.out.println("Current PERSIST_DIR: " + Constants.PERSIST_DIR);
        System.out.println("Current LSM_LEVELS_FILE: " + Constants.LSM_LEVELS_FILE);
        // 初始化内存结构
        this.memtable = Collections.synchronizedList(new ArrayList<>());
        this.immutableMemtables = new LinkedBlockingQueue<>();
        this.lsmLevels = new ConcurrentHashMap<>();
        this.nextSstId = 1;
        this.levelCapacities = new ArrayList<>();
        this.physicalBlocks = new ConcurrentHashMap<>();
        this.freeBlocks = new LinkedList<>();
        this.nextPageNo = 0;
        this.stats = new Stats();

        // 初始化 LSM 层级容量
        for (int i = 0; i < 10; i++) {
            levelCapacities.add((long) (Constants.MAX_MEMTABLE_SIZE * Math.pow(Constants.LEVEL_RATIO, i)));
        }

        // 初始化物理块（优先从磁盘加载，无则新建）
        this.totalBlocks = totalCapacity / Constants.BLOCK_SIZE;
        initPhysicalBlocksFromDisk(totalBlocks);

        // 从磁盘加载持久化数据（SSTable、键范围树）
        loadPersistedData();
        loadMemtableFromDisk();

        // 初始化元数据区（独立于数据区）
        this.metaPhysicalBlocks = new ConcurrentHashMap<>();
        this.metaFreeBlocks = new LinkedList<>();
        initMetaPhysicalBlocksFromDisk(Constants.META_ZONE_BLOCK_COUNT);

        System.out.println("KVSSD_persistence initialized: " +
                "totalBlocks=" + totalBlocks + ", " +
                "loadedSSTCount=" + getTotalSSTCount() + ", " +
                "freeBlocks=" + freeBlocks.size());
    }
    /**
     * 从元数据区分配块（专门用于存储元数据页）
     */
    private PhysicalBlock allocateMetaBlock() {
        // 若元数据区无空闲块，直接扩容（或抛异常，根据需求调整）
        if (metaFreeBlocks.isEmpty()) {
            //    System.err.println("Meta Zone out of space! Expanding...");
            // 简单扩容：新增1个元数据块（实际场景可能需要更复杂的扩容策略）
            long newBlockId = metaPhysicalBlocks.size();
            PhysicalBlock newBlock = new PhysicalBlock(newBlockId);
            metaPhysicalBlocks.put(newBlockId, newBlock);
            metaFreeBlocks.add(newBlockId);
            try {
                savePhysicalBlockToFile(newBlock);
            } catch (IOException e) {
                System.err.println("Failed to expand meta zone: " + e.getMessage());
                return null;
            }
        }

        // 分配元数据块
        long blockId = metaFreeBlocks.poll();
        PhysicalBlock block = metaPhysicalBlocks.get(blockId);
        block.allocated = true;
        block.level = -1; // 元数据区块无需关联层级
        try {
            savePhysicalBlockToFile(block); // 持久化元数据区块状态
        } catch (IOException e) {
            System.err.println("Failed to persist meta block: " + e.getMessage());
        }
        return block;
    }
    /**
     * 初始化元数据区物理块（独立于数据区）
     */
    private void initMetaPhysicalBlocksFromDisk(long totalMetaBlocks) {
        try {
            Files.createDirectories(Paths.get(Constants.META_BLOCK_META_DIR));

            for (long blockId = 0; blockId < totalMetaBlocks; blockId++) {
                File metaFile = new File(Constants.META_BLOCK_META_DIR + blockId + Constants.BLOCK_META_SUFFIX);
                if (metaFile.exists()) {
                    // 加载元数据区块（复用原有加载方法）
                    PhysicalBlock block = loadPhysicalBlockFromFile(blockId, metaFile);
                    metaPhysicalBlocks.put(blockId, block);
                    if (!block.allocated) {
                        metaFreeBlocks.add(blockId);
                    }
                } else {
                    // 新建元数据区块
                    PhysicalBlock newBlock = new PhysicalBlock(blockId);
                    metaPhysicalBlocks.put(blockId, newBlock);
                    metaFreeBlocks.add(blockId);
                    savePhysicalBlockToFile(newBlock); // 持久化到元数据区目录
                }
            }
            System.out.println("Meta Zone initialized: totalBlocks=" + totalMetaBlocks + ", freeBlocks=" + metaFreeBlocks.size());
        } catch (IOException e) {
            throw new RuntimeException("Failed to init meta zone blocks: " + e.getMessage());
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
                    savePhysicalBlockToFile(newBlock);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to init physical blocks from disk: " + e.getMessage());
        }
    }

    /**
     * 从磁盘加载所有持久化数据（SSTable + 键范围树）
     */
    private void loadPersistedData() {
        try {
            // 1. 加载键范围树（明文格式）
            //loadKeyRangeTreeFromDisk();
            loadLsmLevelsFromDisk();

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

    /**
     * 统计 LSM 树中所有层级的 SSTable 总数
     */
    private long getTotalSSTCount() {
        long total = 0;
        for (List<SSTable> ssts : lsmLevels.values()) {
            total += ssts.size();
        }
        return total;
    }
    // ==================== 持久化核心方法（全明文格式）====================
    /**
     * 将 SSTable 持久化到磁盘（纯明文格式，包含完整元数据和KV数据）
     */
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
            writer.write("KEY_RANGE_START=" + sst.keyRange.first);
            writer.newLine();
            writer.write("KEY_RANGE_END=" + sst.keyRange.second);
            writer.newLine();
            writer.write("METADATA_PAGE_PPA=" + sst.metadataPage.ppa);
            writer.newLine();
            writer.write("KV_PAGE_COUNT=" + sst.kvPages.size());
            writer.newLine();
            writer.write("KV_PAIR_COUNT=" + sst.kvpairSize);
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

        //  System.out.println("SSTable persisted (plain text): SST_ID=" + sst.sstId + " | Path=" + sstFilePath);
    }

    /**
     * 从磁盘加载 SSTable（适配明文格式）
     */
    private SSTable loadSSTableFromFile(long sstId, File sstFile) throws IOException {
        if (!sstFile.exists()) {
            System.err.println("SST file missing: " + sstFile.getAbsolutePath());
            return null;
        }

        // 初始化SST基本字段
        SSTable sst = new SSTable(sstId, 0); // 层级后续从文件读取
        sst.keyRange = new Pair<>("", "");
        sst.metadataPage = new PhysicalPage("");
        List<PhysicalPage> kvPages = new ArrayList<>();
        sst.metadataPagePpa = new String();

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
                    } else if (line.startsWith("KEY_RANGE_START=")) {
                        sst.keyRange.first = line.substring("KEY_RANGE_START=".length());
                    } else if (line.startsWith("KEY_RANGE_END=")) {
                        sst.keyRange.second = line.substring("KEY_RANGE_END=".length());
                    } else if (line.startsWith("METADATA_PAGE_PPA=")) {
                        sst.metadataPage.ppa = line.substring("METADATA_PAGE_PPA=".length());
                        sst.metadataPagePpa=line.substring("METADATA_PAGE_PPA=".length());
                    } else if (line.equals("-------------------------- METADATA_PAGE --------------------------")) {
                        parsingHeader = false;
                        parsingMetadataPage = true;
                    }
                    continue;
                }
            }
        }

        // 完善SSTable对象
        sst.pageCounter = 1;

        System.out.println("SSTable loaded (plain text): SST_ID=" + sstId +
                " | Level=" + sst.level + " | meta page ppa=" + sst.metadataPagePpa);
        return sst;
    }

    private void savePhysicalBlockToFile(PhysicalBlock block) throws IOException {
        if (block == null) return;

        // -------------------------- 核心步骤1：创建块专属目录 --------------------------
        // 目录路径：Constants.BLOCK_META_DIR + 块ID（如 "./block_meta/0"）
        File blockDir = new File(Constants.BLOCK_META_DIR + block.blockId);
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
                pageWriter.write("VALID=" + page.valid); // 有效性
                pageWriter.newLine();
                pageWriter.write("REF_COUNT=" + page.refCount); // 引用计数
                pageWriter.newLine();
                pageWriter.write("KEY_RANGE_START=" + page.keyRange.first); // 键范围起始
                pageWriter.newLine();
                pageWriter.write("KEY_RANGE_END=" + page.keyRange.second); // 键范围结束
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
                // 获取前一页的键范围（考虑跨block情况）
                if (pageFileIndex == 0 && block.blockId > 0) {
                    // 当前页是block的第一个页，需要获取前一个block的最后一个页
                    long prevBlockId = block.blockId - 1;
                    PhysicalBlock prevBlock = physicalBlocks.get(prevBlockId);
                    if (prevBlock != null && !prevBlock.pages.isEmpty()) {
                        PhysicalPage prevPage = prevBlock.pages.get(prevBlock.pages.size() - 1);
                        if (prevPage != null && !prevPage.data.isEmpty()) {
                            prevPage.data.sort(Comparator.comparing(p -> p.first));
                            String prevMinKey = prevPage.data.get(0).first;
                            String prevMaxKey = prevPage.data.get(prevPage.data.size() - 1).first;
                            pageWriter.write("PREV_BLOCK_PAGE_" + prevBlockId + "_" + (prevBlock.pages.size() - 1) + "->" +
                                    prevMinKey + "|->|" + prevMaxKey);
                            pageWriter.newLine();
                        }
                    }
                } else if (pageFileIndex > 0) {
                    // 当前block内的前一页
                    PhysicalPage prevPage = block.pages.get(pageFileIndex - 1);
                    if (prevPage != null && !prevPage.data.isEmpty()) {
                        prevPage.data.sort(Comparator.comparing(p -> p.first));
                        String prevMinKey = prevPage.data.get(0).first;
                        String prevMaxKey = prevPage.data.get(prevPage.data.size() - 1).first;
                        pageWriter.write("PREV_PAGE_" + (pageFileIndex - 1) + "->" +
                                prevMinKey + "|->|" + prevMaxKey);
                        pageWriter.newLine();
                    }
                }

                // 获取后一页的键范围（考虑跨block情况）
                if (pageFileIndex == block.pages.size() - 1) {
                    // 当前页是block的最后一个页，需要获取后一个block的第一个页
                    long nextBlockId = block.blockId + 1;
                    PhysicalBlock nextBlock = physicalBlocks.get(nextBlockId);
                    if (nextBlock != null && !nextBlock.pages.isEmpty()) {
                        PhysicalPage nextPage = nextBlock.pages.get(0);
                        if (nextPage != null && !nextPage.data.isEmpty()) {
                            nextPage.data.sort(Comparator.comparing(p -> p.first));
                            String nextMinKey = nextPage.data.get(0).first;
                            String nextMaxKey = nextPage.data.get(nextPage.data.size() - 1).first;
                            pageWriter.write("NEXT_BLOCK_PAGE_" + nextBlockId + "_0" + "->" +
                                    nextMinKey + "|->|" + nextMaxKey);
                            pageWriter.newLine();
                        }
                    }
                } else if (pageFileIndex < block.pages.size() - 1) {
                    // 当前block内的后一页
                    PhysicalPage nextPage = block.pages.get(pageFileIndex + 1);
                    if (nextPage != null && !nextPage.data.isEmpty()) {
                        nextPage.data.sort(Comparator.comparing(p -> p.first));
                        String nextMinKey = nextPage.data.get(0).first;
                        String nextMaxKey = nextPage.data.get(nextPage.data.size() - 1).first;
                        pageWriter.write("NEXT_PAGE_" + (pageFileIndex + 1) + "->" +
                                nextMinKey + "|->|" + nextMaxKey);
                        pageWriter.newLine();
                    }
                }

                pageWriter.write("NEIGHBOR_KEY_RANGES_END");
                pageWriter.newLine();

                // 页尾部标记
                pageWriter.write("==========================================================");
                pageWriter.newLine();
            }

            pageFileIndex++; // 页文件编号递增（下一个页对应下一个编号文件）
        }
    }

    /**
     * 将物理块元数据持久化到磁盘（明文格式）
     */
    private void saveMetaPageToFile(PhysicalBlock block) throws IOException {
        if (block == null) return;

        File metaFile = new File(Constants.META_BLOCK_META_DIR + block.blockId + Constants.BLOCK_META_SUFFIX);
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(metaFile), StandardCharsets.UTF_8))) {

            // 块基本信息
            writer.write("===================== META_BLOCK =====================");
            writer.newLine();
            writer.write("BLOCK_ID=" + block.blockId);
            writer.newLine();
            writer.write("LEVEL=" + block.level);
            writer.newLine();
            writer.write("ALLOCATED=" + block.allocated);
            writer.newLine();
            writer.write("SST_COUNT=" + block.sstables.size());
            writer.newLine();
            writer.write("PAGE_COUNT=" + block.pages.size());
            writer.newLine();
            writer.write("----------------------------------------------------------");
            writer.newLine();

            // 关联的SST列表
            writer.write("ASSOCIATED_SSTS=" + String.join(",",
                    block.sstables.stream().map(String::valueOf).collect(Collectors.toList())));
            writer.newLine();
            writer.newLine();


            // 页信息
            writer.write("PAGES_START");
            writer.newLine();
            int pageIndex = 1;
            for (int i = 0; i < block.pages.size(); i++) {
                PhysicalPage page = block.pages.get(i);
                if (page != null) {
                    // -------------------------- Metadata Page --------------------------
                    writer.write("-------------------------- METADATA_PAGE --------------------------");
                    writer.newLine();
                    writer.write("PPA=" + page.ppa);
                    writer.newLine();
                    writer.write("VALID=" + page.valid);
                    writer.newLine();
                    writer.write("REF_COUNT=" + page.refCount);
                    writer.newLine();
                    writer.write("ENTRY_COUNT=" + page.data.size());
                    writer.newLine();
                    writer.write("FORMAT: KV_PAGE_PPA|KV_PAGE_KEY_RANGE");
                    writer.newLine();
                    writer.write("------------------------------------------------------------------");
                    writer.newLine();
                    // 写入元数据条目
                    for (Pair<String, String> metaEntry : page.data) {
                        // 转义分隔符避免解析错误
                        String escapedPpa = metaEntry.first.replace("|", "||");
                        String escapedRange = metaEntry.second.replace("|", "||");
                        writer.write(escapedPpa + "|" + escapedRange);
                        writer.newLine();
                    }
                    writer.newLine();
                    writer.newLine();
                }
            }
            // 尾部标记
            writer.write("==========================================================");
            writer.newLine();
        }

    }
    /**
     * 从磁盘加载物理块元数据（明文格式）
     */
    private PhysicalBlock loadPhysicalBlockFromFile(long blockId, File metaFile) throws IOException {
        PhysicalBlock block = new PhysicalBlock(blockId);

        boolean parsingPages = false;
        PhysicalPage currentPage = null;
        int currentPageNo = -1;
        boolean parsingKvPairs = false;
        boolean parsingNeighborRanges = false; // 新增：解析相邻页键范围状态
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
                        } else if (line.startsWith("VALID=")) {
                            currentPage.valid = Boolean.parseBoolean(line.substring("VALID=".length()).trim());
                        } else if (line.startsWith("REF_COUNT=")) {
                            currentPage.refCount = Integer.parseInt(line.substring("REF_COUNT=".length()).trim());
                        } else if (line.startsWith("KEY_RANGE_START=")) {
                            String startKey = line.substring("KEY_RANGE_START=".length()).trim();
                            currentPage.keyRange = new Pair<>(startKey, currentPage.keyRange.second);
                        } else if (line.startsWith("KEY_RANGE_END=")) {
                            String endKey = line.substring("KEY_RANGE_END=".length()).trim();
                            currentPage.keyRange = new Pair<>(currentPage.keyRange.first, endKey);
                        } else if (line.equals("KV_PAIRS_START")) {
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
                    // 解析相邻页键范围数据
                    else if (parsingNeighborRanges && currentPage != null) {
                        if (line.equals("NEIGHBOR_KEY_RANGES_END")) {
                            parsingNeighborRanges = false;
                        } else if (!line.equals("KV_PAIRS_END")) {
                            //TODO 解析相邻页键范围数据
                            // 这里可以处理相邻页的键范围信息
                            // 目前只是跳过这些行，实际应用中可以根据需要存储这些信息
                            System.out.println("Loaded neighbor key range: " + line);
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
    /**
     * 将LSM层级结构（level -> SSTable列表）持久化到磁盘（明文格式）
     */
    private void saveLsmLevelsToDisk() throws IOException {
        if (isPersisting) return;
        isPersisting = true;

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
        } finally {
            isPersisting = false;
        }
        //  System.out.println("LSM levels persisted: " + lsmLevels.size() + " levels");
    }
    /**
     * 从磁盘加载LSM层级结构（level -> SSTable列表）
     * 注意：需在加载所有SSTable之后调用（依赖已加载的SSTable对象）
     */
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
    // ==================== 原有核心逻辑改造（加入持久化调用）====================
    /**
     * 生成物理页地址（PPA）
     */
    private String generatePPA(long blockId) {
        String ppa = blockId + "_" + nextPageNo;
        nextPageNo = (nextPageNo + 1) % (int)(Constants.BLOCK_SIZE / Constants.PAGE_SIZE);
        return ppa;
    }

    /**
     * 分配物理块（改造：提前触发GC，避免GC时无块可用）
     */
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
                // 持久化块的分配状态变更
                try {
                    savePhysicalBlockToFile(block);
                } catch (IOException e) {
                    System.err.println("Failed to persist block after allocate: " + e.getMessage());
                }
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
        try {
            savePhysicalBlockToFile(block);
        } catch (IOException e) {
            System.err.println("Failed to persist new block: " + e.getMessage());
        }
        return block;
    }
//    /**
//     * 将 Memtable 写入 SSTable（改造：加入 SST 持久化）
//     */
//    private SSTable writeMemtableToSSTable(List<Pair<String, String>> memtable, int level) {
//        System.out.println("now write no."+nextSstId+" sstable.");
//        SSTable sst = new SSTable(nextSstId++, level);
//        sst.kvpairSize = memtable.size();
//        System.out.println("memtable size:"+memtable.size());
//        // 1. Memtable 排序
//        List<Pair<String, String>> sortedMemtable = new ArrayList<>(memtable);
//        sortedMemtable.sort(Comparator.comparing(p -> p.first));
//        System.out.println("sortedmemtable size:"+sortedMemtable.size());
////        for(Pair<String, String> kv : sortedMemtable){
////            System.out.println("key :"+kv.first);
////        }
//        // 2. 分配物理块
//        PhysicalBlock block = allocateBlock(level);
//        if (block == null) {
//            System.out.println("dead here.");
//            return null;
//        }
//        // 3. 拆分 KV 页
//        List<PhysicalPage> kvPages = new ArrayList<>();
//        List<Pair<String, String>> pageKvs = new ArrayList<>();
//        int pageSizeUsed = 0;
//        for (Pair<String, String> kv : sortedMemtable) {
//            int kvSize = kv.first.getBytes(StandardCharsets.UTF_8).length +
//                    kv.second.getBytes(StandardCharsets.UTF_8).length;
//            if (pageSizeUsed + kvSize > Constants.PAGE_SIZE) {
//                // 页满，创建并添加物理页
//                String ppa = generatePPA(block.blockId);
//                PhysicalPage page = new PhysicalPage(ppa);
//                page.data = new ArrayList<>(pageKvs);
//                page.updateKeyRange();
//
//                System.out.println("page "+nextPageNo+" key range:"+page.keyRange.first+" "+page.keyRange.second);
//                int pageNo = Integer.parseInt(ppa.split("_")[1]);
//                if (block.addPage(pageNo, page)) {
//                    kvPages.add(page);
//                    stats.totalFlashWrites += Constants.PAGE_SIZE;
//                }
//
//                pageKvs.clear();
//                pageSizeUsed = 0;
//            }
//
//            pageKvs.add(kv);
//            pageSizeUsed += kvSize;
//        }
//        // 4. 处理最后一页
//        if (!pageKvs.isEmpty()) {
//            String ppa = generatePPA(block.blockId);
//            PhysicalPage page = new PhysicalPage(ppa);
//            page.data = new ArrayList<>(pageKvs);
//
//            page.updateKeyRange();
//            System.out.println("page  key range:"+page.keyRange.first+" "+page.keyRange.second);
//            int pageNo = Integer.parseInt(ppa.split("_")[1]);
//            if (block.addPage(pageNo, page)) {
//                kvPages.add(page);
//                stats.totalFlashWrites += Constants.PAGE_SIZE;
//            }
//            pageKvs.clear();
//            pageSizeUsed = 0;
//        }
//        // 5. 创建元数据页（修改后：写入元数据区）
//        // 5.1 从元数据区分配块（确保独立存储）
//        PhysicalBlock metaBlock = allocateMetaBlock();
//        if (metaBlock == null) {
//            System.err.println("Failed to allocate meta block for metadata page");
//            return null;
//        }
//        // 5.2 生成元数据页的PPA（基于元数据区块）
//        String metaPpa = generatePPA(metaBlock.blockId); // 复用原有PPA生成逻辑
//        sst.metadataPagePpa=metaPpa; // Meta Page指针（如"0_0"）
//        // 5.3 初始化元数据页并写入KV页索引
//        PhysicalPage metaPage = new PhysicalPage(metaPpa);
//        for (PhysicalPage page : kvPages) {
//            metaPage.data.add(new Pair<>(page.ppa,
//                    page.keyRange.first + "|" + page.keyRange.second));
//        }
//        // 5.4 将元数据页添加到元数据区块（而非数据块）
//        int metaPageNo = Integer.parseInt(metaPpa.split("_")[1]);
//        if (metaBlock.addPage(metaPageNo, metaPage)) {
//            sst.metadataPage = metaPage;
//            stats.totalFlashWrites += Constants.PAGE_SIZE; // 统计元数据页写入
//            // 关联元数据块与SSTable
//            metaBlock.sstables.add(sst.sstId);
//            try {
//                savePhysicalBlockToFile(metaBlock); // 持久化元数据区块变更
//            } catch (IOException e) {
//                System.err.println("Failed to persist meta block after adding metadata page: " + e.getMessage());
//            }
//        } else {
//            // 元数据区块添加失败时，释放块并返回错误
//            metaBlock.allocated = false;
//            metaFreeBlocks.add(metaBlock.blockId);
//            System.err.println("Failed to add metadata page to meta block");
//            return null;
//        }
//        // 6. 完善 SSTable 信息
//        // sst.kvPages = kvPages;
//        sst.updateKeyRange(kvPages);
//        block.sstables.add(sst.sstId);
//        // 7. 加入 LSM 层级
//        List<SSTable> levelList = lsmLevels.computeIfAbsent(level, k -> new ArrayList<>());
//        levelList.add(sst);
//        // 按 min key 排序
//        levelList.sort(Comparator.comparing(s -> s.keyRange.first));
//        // 8. 核心改造：持久化 SSTable 和物理块元数据
//        try {
//            System.out.println("now  write no."+(nextSstId-1)+"sstable done.");
//            saveSSTableToFile(sst);
//            savePhysicalBlockToFile(block);
//            saveMetaPageToFile(metaBlock);
//            // saveKeyRangeTreeToDisk();
//        } catch (IOException e) {
//            System.err.println("Failed to persist SSTable/block: " + e.getMessage());
//            return null;
//        }
//        return sst;
//    }
// 假设 Pair, SSTable, PhysicalBlock, PhysicalPage, Constants 等类已在别处定义。

    /**
     * 将 Memtable 写入 SSTable，并处理一个 Block 写满的情况。
     * 当一个 Block（例如128页）被写满时，此方法会停止向当前 SSTable 添加数据。
     * 任何未成功写入的键值对都会被重新填充到传入的 `memtable` 列表中，
     * 以便调用者可以对这些剩余的数据进行下一次刷盘操作。
     *
     * @param memtable 包含待写入键值对的列表。此列表在方法执行后将被修改，仅包含未成功写入的键值对。
     * @param level 目标LSM层级。
     * @return 如果成功写入了至少一个页面，则返回创建的 SSTable；否则返回 null。
     */
    private SSTable writeMemtableToSSTable(List<Pair<String, String>> memtable, int level) {
        System.out.println("正在写入 SSTable，编号: " + nextSstId);
        SSTable sst = new SSTable(nextSstId++, level);

        // 1. 对 Memtable 进行排序
        List<Pair<String, String>> sortedMemtable = new ArrayList<>(memtable);
        sortedMemtable.sort(Comparator.comparing(p -> p.first));

        for (Pair<String, String> kv : sortedMemtable) {
            if(kv.first.equals("user350269079075")){
                System.out.println("user350269079075 in sortedMemtable "+kv);
            }
        }
        // 清空原始 memtable，准备接收任何处理后剩余的 KV
        memtable.clear();

        // 2. 分配物理块
        PhysicalBlock block = allocateBlock(level);
        if (block == null) {
            System.out.println("分配块失败。所有 KV 将返回到 memtable。");
            // 无法分配块，所有数据都算作剩余数据
            memtable.addAll(sortedMemtable);
            return null;
        }

        // 3. 将 KV 对拆分到页中
        List<PhysicalPage> kvPages = new ArrayList<>();
        List<Pair<String, String>> pageKvs = new ArrayList<>();
        int pageSizeUsed = 0;
        int i = 0; // 使用索引遍历，以便记录中断点
        nextPageNo = 0;
        for (i = 0; i < sortedMemtable.size(); i++) {
            Pair<String, String> kv = sortedMemtable.get(i);
            int kvSize = kv.first.getBytes(StandardCharsets.UTF_8).length +
                    kv.second.getBytes(StandardCharsets.UTF_8).length;
            if(kv.first=="user350269079075"){
                System.out.println("user350269079075 in page "+nextPageNo );
                continue;
            }

            if (pageSizeUsed > 0 && pageSizeUsed + kvSize > Constants.PAGE_SIZE) {
                // 当前页已满，创建并尝试添加物理页
                String ppa = generatePPA(block.blockId);
                PhysicalPage page = new PhysicalPage(ppa);
                page.data = new ArrayList<>(pageKvs);
                page.updateKeyRange();

                int pageNo = Integer.parseInt(ppa.split("_")[1]);
                if (block.addPage(pageNo, page)) {
                    kvPages.add(page);
                    stats.totalFlashWrites += Constants.PAGE_SIZE;
                    pageKvs.clear();
                    pageSizeUsed = 0;
                } else {
                    // Block 已满，无法添加新页。中断循环。
                    System.out.println("Block 已满。停止写入 SSTable。");
                    break;
                }
            }
            pageKvs.add(kv);
            pageSizeUsed += kvSize;
        }

        // 4. 循环结束后的处理
        if (i < sortedMemtable.size()) {
            nextPageNo=0;
            this.memtable.addAll(pageKvs);
            //TODO memtablef的大小改一改，说不定还会触发flush
            for(int j=i;j<sortedMemtable.size();j++){
                this.memtable.add(sortedMemtable.get(j));
                if(sortedMemtable.get(j).first=="user350269079075"){
                    System.out.println("user350269079075 in memtable "+j);
                }
            }



        } else if (!pageKvs.isEmpty()) {
            // 循环正常结束，处理最后一页
            String ppa = generatePPA(block.blockId);
            PhysicalPage page = new PhysicalPage(ppa);
            page.data = new ArrayList<>(pageKvs);
            page.updateKeyRange();

            int pageNo = Integer.parseInt(ppa.split("_")[1]);
            if (block.addPage(pageNo, page)) {
                System.out.println("最后一页写入成功。所有 KV 都已处理。");
                kvPages.add(page);
                stats.totalFlashWrites += Constants.PAGE_SIZE;
                // memtable 保持为空，因为没有剩余数据
            } else {
                // Block 已满，最后一页也无法写入
                System.out.println("Block 已满，无法写入最后一页。将返回 " + pageKvs.size() + " 个 KV 到 memtable。");
                memtable.addAll(pageKvs);
            }
        }

        // 如果没有页面被写入，说明SSTable为空，这是一个无效操作
        if (kvPages.isEmpty()) {
            System.err.println("没有任何页被写入 SSTable。中止创建。");
            block.allocated = false; // 归还 block (需要实现具体的归还逻辑)
            // metaFreeBlocks.add(block.blockId);
            return null;
        }

        sst.kvpairSize = sortedMemtable.size() - memtable.size(); // 实际写入的KV数量

        // 5. 创建元数据页... (后续逻辑基本不变)
        PhysicalBlock metaBlock = allocateMetaBlock();
        if (metaBlock == null) {
            System.err.println("为元数据页分配元数据块失败");
            // 实际应用中需要更复杂的错误处理/回滚
            return null;
        }

        String metaPpa = generatePPA(metaBlock.blockId);
        sst.metadataPagePpa = metaPpa;
        PhysicalPage metaPage = new PhysicalPage(metaPpa);
        for (PhysicalPage p : kvPages) {
            metaPage.data.add(new Pair<>(p.ppa, p.keyRange.first + "|" + p.keyRange.second));
        }

        int metaPageNo = Integer.parseInt(metaPpa.split("_")[1]);
        if (metaBlock.addPage(metaPageNo, metaPage)) {
            sst.metadataPage = metaPage;
            stats.totalFlashWrites += Constants.PAGE_SIZE;
            metaBlock.sstables.add(sst.sstId);
            try {
                savePhysicalBlockToFile(metaBlock);
            } catch (IOException e) {
                System.err.println("持久化元数据块失败: " + e.getMessage());
            }
        } else {
            metaBlock.allocated = false;
            // metaFreeBlocks.add(metaBlock.blockId);
            System.err.println("未能将元数据页添加到元数据块");
            return null;
        }

        // 6. 完善 SSTable 信息
        sst.updateKeyRange(kvPages);
        block.sstables.add(sst.sstId);

        // 7. 加入 LSM 层级
        List<SSTable> levelList = lsmLevels.computeIfAbsent(level, k -> new ArrayList<>());
        levelList.add(sst);
        levelList.sort(Comparator.comparing(s -> s.keyRange.first));

        // 8. 持久化 SSTable 和物理块元数据
        try {
            System.out.println("编号为 " + (nextSstId - 1) + " 的 sstable 写入完成，包含 " + sst.kvpairSize + " 个 KV。");
            saveSSTableToFile(sst);
            savePhysicalBlockToFile(block);
            saveMetaPageToFile(metaBlock);
        } catch (IOException e) {
            System.err.println("持久化 SSTable/block 失败: " + e.getMessage());
            return null;
        }
        return sst;
    }
    /**
     * 检查层级压缩（原有逻辑保留）
     */
    private int getLevelSstLimit(int level) {
        if (level < Constants.LEVEL_SST_COUNT_LIMITS.length) {
            return Constants.LEVEL_SST_COUNT_LIMITS[level];
        }
        return Constants.LEVEL_SST_COUNT_LIMITS[Constants.LEVEL_SST_COUNT_LIMITS.length - 1];
    }

    private void checkLevelCompaction(int level) {
        List<SSTable> currentLevelSsts = lsmLevels.getOrDefault(level, new ArrayList<>());
        int levelSstLimit = getLevelSstLimit(level); // 当前层级的 SSTable 数量阈值

        // 循环触发 Compaction，直到当前层级 SSTable 数量 ≤ 阈值（修复原逻辑引用不刷新问题）
        while (!currentLevelSsts.isEmpty() && currentLevelSsts.size() > levelSstLimit) {
            System.out.println("=== 触发层级 " + level + " 的 Compaction ===");
            System.out.println("当前数量：" + currentLevelSsts.size() + "，阈值：" + levelSstLimit);
            // -------------------------- L1+：单个迁移到下一层（level+1） --------------------------
            // 1. 提取要迁移的 SSTable（最旧的第 1 个）
            int excess=currentLevelSsts.size()-levelSstLimit;
            List<SSTable> victimSsts = new ArrayList<>();
            for (int i = 0; i < excess; i++) {
                victimSsts.add(currentLevelSsts.get(i)); // 取前excess个作为victim（超出多少选多少）
            }
            // SSTable victimSst = currentLevelSsts.get(0);
            // 步骤2：获取目标层级（level+1）的所有有效SSTable
            // 从lsmLevels中获取目标层级的原始SSTable列表（无则为空列表）
            List<SSTable> targetLevelSsts = lsmLevels.get(level + 1);
            // 步骤3：查找目标层级中与受害者SST重叠的SSTable（论文3.C：重叠才需合并）
            List<SSTable> overlappingSsts = new ArrayList<>();
            overlappingSsts.addAll(victimSsts); // 先加入受害者自身
            System.out.println("level :"+level);
            if(targetLevelSsts!=null){
//                for (SSTable targetSst : targetLevelSsts) {
//                    // 判断键范围是否重叠（调用原KeyRangeComparator工具类）
//                    if (keyrangeComparator.isOverlapping(victimSst.keyRange, targetSst.keyRange)) {
//                        overlappingSsts.add(targetSst);
//                        //    System.out.printf("目标层级%d中重叠SST：ID=%d，键范围=[%s, %s]%n",
//                        //            level + 1,
//                        //            targetSst.sstId,
//                        //            new String(targetSst.keyRange.first),
//                        //            new String(targetSst.keyRange.second));
//                    }
//                }
                for (SSTable targetSst : targetLevelSsts) {
                    // 检查目标SST是否与任何一个victim重叠
                    boolean isOverlapping = false;
                    for (SSTable victim : victimSsts) {
                        if (keyrangeComparator.isOverlapping(victim.keyRange, targetSst.keyRange)) {
                            isOverlapping = true;
                            break;
                        }
                    }
                    if (isOverlapping) {
                        overlappingSsts.add(targetSst); // 加入所有重叠的目标SST
                    }
                }
            }

            // 步骤3：执行合并Compaction（核心：生成无重叠新SSTable和新MetaPage）
            newRunCompaction(level, overlappingSsts);
            // 2. 执行迁移（创建新 SSTable 加入下一层，删除当前层原 SSTable）
            // runCompaction(level, victimSst);
            // 4. 刷新当前层级引用（确保循环收敛）
            currentLevelSsts = lsmLevels.getOrDefault(level, new ArrayList<>());
            checkLevelCompaction(level + 1); // 检查下一层是否因新增超阈值
        }


    }
    public void sortAllKvPageEntriesByMinKeyAsc(List<Pair<String, String>> allKvPageEntries) {
        if (allKvPageEntries == null || allKvPageEntries.isEmpty()) {
            return; // 空列表无需排序
        }

        // 使用Collections.sort + 自定义Comparator，按最小键升序排序
        Collections.sort(allKvPageEntries, new Comparator<Pair<String, String>>() {
            @Override
            public int compare(Pair<String, String> p1, Pair<String, String> p2) {
                // 1. 从键范围字符串中提取最小键（small_key）
                String minKey1 = extractMinKey(p1.second);
                String minKey2 = extractMinKey(p2.second);

                // 2. 按最小键升序排序（使用String.compareTo实现自然顺序）
                // 例如："key10" < "key20" → 升序排列
                return minKey1.compareTo(minKey2);
            }

            /**
             * 辅助方法：从键范围字符串中提取最小键（small_key）
             * @param keyRange 键范围字符串，格式为"small_key|max_key"
             * @return 提取的最小键；若格式错误，返回原字符串（避免排序异常）
             */
            private String extractMinKey(String keyRange) {
                if (keyRange == null || keyRange.isEmpty()) {
                    return ""; // 空字符串按最小处理
                }
                // 拆分"small_key|max_key"，取第一个部分作为最小键
                String[] keyParts = keyRange.split("\\|"); // 注意转义竖线（|在正则中为特殊字符）
                return keyParts.length >= 1 ? keyParts[0].trim() : keyRange.trim();
            }
        });
    }


    private List<SSTable> splitIntoNonOverlappingSsts(List<Pair<String, String>> allKvPageEntries, int targetLevel) {

        List<SSTable> newSsts = new ArrayList<>();
        if (allKvPageEntries.isEmpty()) {
            return newSsts; // 无KV页时返回空列表，避免空指针
        }

        // 1. 核心参数：SSTable大小阈值（论文3.B“Flash块对齐”设计）
        // 论文3.2节提到“nLSM树为SSTable独占一个Flash块”，此处按Flash块大小设置SSTable最大KV页数
        int maxKvPagesPerSst = (int) (Constants.BLOCK_SIZE / Constants.PAGE_SIZE); // 假设Flash块含128个32KB页（论文2.2节FTL设计：块=128页）
        List<Pair<String, String>> currentSstKvEntries = new ArrayList<>(); // 当前正在构建的SSTable的KV页条目

        // 2. 遍历排序后的KV页条目，按规则拆分（论文3.C“合并排序+无重叠拆分”逻辑）
        for (int i = 0; i < allKvPageEntries.size(); i++) {
            Pair<String, String> currentPair = allKvPageEntries.get(i);
            currentSstKvEntries.add(currentPair); // 将当前KV页加入待构建的SSTable

            // 3. 拆分触发条件（满足任一即拆分，确保新SSTable无重叠且符合Flash块大小）
            boolean needSplit = false;
            // 条件1：当前SSTable的KV页数达到阈值（论文3.B“Flash块独占”：避免跨块存储）
            if (currentSstKvEntries.size() >= maxKvPagesPerSst) {
                needSplit = true;
            }
            // 条件2：下一个KV页的键范围与当前SSTable的键范围无重叠（论文3.C“无重叠原则”）
            // （仅当不是最后一个KV页时判断）
            // if (!needSplit && i < allKvPageEntries.size() - 1) {
            //     Pair<String, String> nextKvEntry = allKvPageEntries.get(i + 1);
            //     String[] nextKeyRangeParts = nextKvEntry.second.split("\\|"); // 注意转义竖线（|在正则中需转义）
            //    // 格式错误时跳过该KV页，避免影响后续拆分
            //     if (nextKeyRangeParts.length != 2) {continue;}
            //     String nextMinKey = nextKeyRangeParts[0].trim(); // 去除空格，确保键比较准确
            //     String nextMaxKey = nextKeyRangeParts[1].trim();
            //     // 获取当前待构建SSTable的键范围（最小键=第一个KV页最小键，最大键=当前最后一个KV页最大键）
            //     String[] currentKeyRangeParts = currentPair.second.split("\\|"); // 注意转义竖线（|在正则中需转义）
            //     String currentMinKey = currentKeyRangeParts[0].trim(); // 去除空格，确保键比较准确
            //     String currentMaxKey = currentKeyRangeParts[1].trim();
            //     // 若当前SSTable的最大键 < 下一个KV页的最小键 → 无重叠，可拆分
            //     if (keyrangeComparator.compare(currentMaxKey, nextMinKey) < 0) {
            //         needSplit = true;
            //     }
            // }
            // 条件3：已遍历到最后一个KV页（必须拆分，避免遗漏）
            if (!needSplit && i == allKvPageEntries.size() - 1) {
                needSplit = true;
            }

            // 4. 触发拆分：构建新SSTable及对应的MetaPage（论文3.B“MetaPage管理KV页”）
            if (needSplit) {
                // 4.1 生成新SSTable（分配唯一ID，指定目标层级）
                SSTable newSst = new SSTable(nextSstId++, targetLevel); // nextSstId为全局自增ID，确保唯一性

                // 4.2 构建新SSTable的MetaPage（论文3.B：MetaPage存储KV页PPA和键范围）
                PhysicalBlock metaBlock = allocateMetaBlock();
                String metaPpa = generatePPA(metaBlock.blockId); // 复用原有PPA生成逻辑
                newSst.metadataPagePpa=metaPpa;
                PhysicalPage newMetaPage = new PhysicalPage(metaPpa);
                for ( Pair<String, String> kvEntry : currentSstKvEntries) {
                    newMetaPage.data.add(kvEntry); // 将KV页条目加入新MetaPage
                }
                int metaPageNo = Integer.parseInt(metaPpa.split("_")[1]);
                if (metaBlock.addPage(metaPageNo, newMetaPage)) {
                    newSst.metadataPage = newMetaPage;
                    stats.totalFlashWrites += Constants.PAGE_SIZE; // 统计元数据页写入
                    // 关联元数据块与SSTable
                    metaBlock.sstables.add(newSst.sstId);
                    try {
                        savePhysicalBlockToFile(metaBlock); // 持久化元数据区块变更
                    } catch (IOException e) {
                        System.err.println("Failed to persist meta block after adding metadata page: " + e.getMessage());
                    }
                } else {
                    // 元数据区块添加失败时，释放块并返回错误
                    metaBlock.allocated = false;
                    metaFreeBlocks.add(metaBlock.blockId);
                    System.err.println("Failed to add metadata page to meta block");
                    return null;
                }
                // 4.3 计算新SSTable的键范围（论文3.C：SSTable键范围=所有KV页键范围的并集）
                String[] newSstKeyRangeParts = currentSstKvEntries.get(0).second.split("\\|"); // 注意转义竖线（|在正则中需转义）
                String newSstMinKey = newSstKeyRangeParts[0].trim(); // 去除空格，确保键比较准确
                //@TODO max错了，最后一个不一定是maxKey最大的！
                Collections.sort(currentSstKvEntries, new Comparator<Pair<String, String>>() {
                    @Override
                    public int compare(Pair<String, String> entry1, Pair<String, String> entry2) {
                        String keyMax1 = extractKeyMax(entry1.second);
                        String keyMax2 = extractKeyMax(entry2.second);
                        return keyMax2.compareTo(keyMax1); // 降序
                    }
                    private String extractKeyMax(String keyRange) {
                        return (keyRange != null && keyRange.contains("|")) ?
                                keyRange.split("\\|")[1].trim() : "";
                    }
                });
                String[] KeyRangeParts = currentSstKvEntries.get(0).second.split("\\|");
                String newSstMaxKey = KeyRangeParts[1].trim();
                newSst.keyRange = new Pair<>(newSstMinKey, newSstMaxKey); // 赋值SSTable键范围
                // 4.4 填充新SSTable的其他核心属性（论文3.B）
                //newSst.metadataPage = newMetaPage; // 关联MetaPage
                newSst.metadataPagePpa = newMetaPage.ppa; // 关联MetaPage的Flash物理地址（PPA）
                // newSst.kvpairSize = calculateTotalKvCount(currentSstKvEntries); // 统计SSTable总KV对数（遍历KV页计算）

                try {
                    savePhysicalBlockToFile(metaBlock);
                    saveMetaPageToFile(metaBlock);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    //  e.printStackTrace();
                }
                // 4.5 将新SSTable加入结果列表
                newSsts.add(newSst);

                // 4.6 重置当前待构建SSTable的KV页列表，准备下一个SSTable
                currentSstKvEntries.clear();
            }
        }

        return newSsts;
    }

    private List<SSTable> newSplitIntoNonOverlappingSsts(List<Pair<String, String>> allKvPageEntries, int targetLevel){
        List<SSTable> newSSTables = new ArrayList<>();
        List<Pair<String, String>> currentGroup = new ArrayList<>();
        int maxKvPagesPerSst = (int) (Constants.BLOCK_SIZE / Constants.PAGE_SIZE);
        int currentCount = 0;
        for (int i = 0; i < allKvPageEntries.size(); i++) {
            Pair<String, String> currentPair = allKvPageEntries.get(i);
            currentGroup.add(currentPair); // 将当前KV页加入待构建的SSTable
            currentCount++;
            boolean needSplit = currentGroup.size() >= maxKvPagesPerSst || i == allKvPageEntries.size() - 1;

            if (needSplit){
                List<Pair<String, String>> sharedPages = new ArrayList<>();
                if (i != allKvPageEntries.size() - 1){
                    Pair<String,String> next = allKvPageEntries.get(i + 1);
                    //boolean overlap = (currentPair.second.split("\\|")[1].compareTo(next.second.split("\\|")[0])>0 );
                    for(Pair<String,String> entry:currentGroup){
                        if(entry.second.split("\\|")[1].compareTo(next.second.split("\\|")[0])>0){
                            sharedPages.add(entry);
                        }
                    }
                    SSTable newSst = new SSTable(nextSstId++, targetLevel);
                    PhysicalBlock metaBlock = allocateMetaBlock();
                    String metaPpa = generatePPA(metaBlock.blockId);
                    newSst.metadataPagePpa=metaPpa;
                    PhysicalPage newMetaPage = new PhysicalPage(metaPpa);
                    for ( Pair<String, String> kvEntry : currentGroup) {
                        newMetaPage.data.add(kvEntry); // 将KV页条目加入新MetaPage
                    }
                    int metaPageNo = Integer.parseInt(metaPpa.split("_")[1]);
                    if (metaBlock.addPage(metaPageNo, newMetaPage)) {
                        newSst.metadataPage = newMetaPage;
                        stats.totalFlashWrites += Constants.PAGE_SIZE; // 统计元数据页写入
                        // 关联元数据块与SSTable
                        metaBlock.sstables.add(newSst.sstId);
                        try {
                            savePhysicalBlockToFile(metaBlock); // 持久化元数据区块变更
                        } catch (IOException e) {
                            System.err.println("Failed to persist meta block after adding metadata page: " + e.getMessage());
                        }
                    } else {
                        // 元数据区块添加失败时，释放块并返回错误
                        metaBlock.allocated = false;
                        metaFreeBlocks.add(metaBlock.blockId);
                        System.err.println("Failed to add metadata page to meta block");
                        return null;
                    }
                    // 4.3 计算新SSTable的键范围（论文3.C：SSTable键范围=所有KV页键范围的并集）
                    String[] newSstKeyRangeParts = currentGroup.get(currentGroup.size()-(int) (Constants.BLOCK_SIZE / Constants.PAGE_SIZE)).second.split("\\|"); // 注意转义竖线（|在正则中需转义）
                    String newSstMinKey = newSstKeyRangeParts[0].trim(); // 去除空格，确保键比较准确
                    String newSstMaxKey = next.second.split("\\|")[0].trim();
                    newSst.keyRange = new Pair<>(newSstMinKey, newSstMaxKey); // 赋值SSTable键范围
                    newSst.metadataPagePpa = newMetaPage.ppa; // 关联MetaPage的Flash物理地址（PPA）
                    try {
                        savePhysicalBlockToFile(metaBlock);
                        saveMetaPageToFile(metaBlock);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        //  e.printStackTrace();
                    }
                    newSSTables.add(newSst);
                    currentGroup = new ArrayList<>();
                    for (Pair<String ,String> entry:sharedPages){
                        currentGroup.add(entry);
                    }
                    currentCount = 0;
                }
                else {

                    SSTable newSst = new SSTable(nextSstId++, targetLevel); // nextSstId为全局自增ID，确保唯一性
                    PhysicalBlock metaBlock = allocateMetaBlock();
                    String metaPpa = generatePPA(metaBlock.blockId); // 复用原有PPA生成逻辑
                    newSst.metadataPagePpa=metaPpa;
                    PhysicalPage newMetaPage = new PhysicalPage(metaPpa);
                    for ( Pair<String, String> kvEntry : currentGroup) {
                        newMetaPage.data.add(kvEntry); // 将KV页条目加入新MetaPage
                    }
                    int metaPageNo = Integer.parseInt(metaPpa.split("_")[1]);
                    if (metaBlock.addPage(metaPageNo, newMetaPage)) {
                        newSst.metadataPage = newMetaPage;
                        stats.totalFlashWrites += Constants.PAGE_SIZE; // 统计元数据页写入
                        metaBlock.sstables.add(newSst.sstId);
                        try {
                            savePhysicalBlockToFile(metaBlock); // 持久化元数据区块变更
                        } catch (IOException e) {
                            System.err.println("Failed to persist meta block after adding metadata page: " + e.getMessage());
                        }
                    } else {
                        // 元数据区块添加失败时，释放块并返回错误
                        metaBlock.allocated = false;
                        metaFreeBlocks.add(metaBlock.blockId);
                        System.err.println("Failed to add metadata page to meta block");
                        return null;
                    }
                    String[] newSstKeyRangeParts = currentGroup.get(0).second.split("\\|"); // 注意转义竖线（|在正则中需转义）
                    String newSstMinKey = newSstKeyRangeParts[0].trim();
                    Collections.sort(currentGroup, new Comparator<Pair<String, String>>() {
                        @Override
                        public int compare(Pair<String, String> entry1, Pair<String, String> entry2) {
                            String keyMax1 = extractKeyMax(entry1.second);
                            String keyMax2 = extractKeyMax(entry2.second);
                            return keyMax2.compareTo(keyMax1); // 降序
                        }
                        private String extractKeyMax(String keyRange) {
                            return (keyRange != null && keyRange.contains("|")) ?
                                    keyRange.split("\\|")[1].trim() : "";
                        }
                    });
                    String[] KeyRangeParts = currentGroup.get(0).second.split("\\|");
                    String newSstMaxKey = KeyRangeParts[1].trim();
                    newSst.keyRange = new Pair<>(newSstMinKey, newSstMaxKey); // 赋值SSTable键范围
                    newSst.metadataPagePpa = newMetaPage.ppa; // 关联MetaPage的Flash物理地址（PPA）
                    try {
                        savePhysicalBlockToFile(metaBlock);
                        saveMetaPageToFile(metaBlock);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        //  e.printStackTrace();
                    }
                    newSSTables.add(newSst);
                    // 清空 currentGroup，继续（循环会结束）
                    currentGroup = new ArrayList<>();
                    currentCount = 0;
                }
            }
        }
        return newSSTables;
    }


    private List<SSTable> newnewSplitIntoNonOverlappingSsts(List<Pair<String, String>> allKvPageEntries, int targetLevel){
        List<SSTable> newSSTables = new ArrayList<>();
        List<Pair<String, String>> currentGroup = new LinkedList<>();
        int maxKvPagesPerSst = (int) (Constants.BLOCK_SIZE / Constants.PAGE_SIZE);
        int currentCount = 0;
        for (int i = 0; i < allKvPageEntries.size(); i++) {
            Pair<String, String> currentPair = allKvPageEntries.get(i);
            currentGroup.add(currentPair); // 将当前KV页加入待构建的SSTable
            currentCount++;
            boolean needSplit = currentCount >= maxKvPagesPerSst || i == allKvPageEntries.size() - 1;

            if (needSplit){
                List<Pair<String, String>> sharedPages = new LinkedList<>();
                if (i != allKvPageEntries.size() - 1){
                    Pair<String,String> next = allKvPageEntries.get(i + 1);
                    String[] nextKeyParts = next.second.split("\\|", 2); // 限制拆分次数为2
                    String nextMinKey = nextKeyParts[0].trim();

                    for(Pair<String,String> entry:currentGroup){
                        if(entry.second.split("\\|")[1].compareTo(nextMinKey)>0){
                            sharedPages.add(entry);
                        }
                    }
                    SSTable newSst = new SSTable(nextSstId++, targetLevel);
                    PhysicalBlock metaBlock = allocateMetaBlock();
                    String metaPpa = generatePPA(metaBlock.blockId);
                    newSst.metadataPagePpa=metaPpa;
                    PhysicalPage newMetaPage = new PhysicalPage(metaPpa);
                    for ( Pair<String, String> kvEntry : currentGroup) {
                        newMetaPage.data.add(kvEntry); // 将KV页条目加入新MetaPage
                    }
                    int metaPageNo = Integer.parseInt(metaPpa.split("_")[1]);
                    if (metaBlock.addPage(metaPageNo, newMetaPage)) {
                        newSst.metadataPage = newMetaPage;
                        stats.totalFlashWrites += Constants.PAGE_SIZE; // 统计元数据页写入
                        // 关联元数据块与SSTable
                        metaBlock.sstables.add(newSst.sstId);
                        try {
                            savePhysicalBlockToFile(metaBlock); // 持久化元数据区块变更
                        } catch (IOException e) {
                            System.err.println("Failed to persist meta block after adding metadata page: " + e.getMessage());
                        }
                    } else {
                        // 元数据区块添加失败时，释放块并返回错误
                        metaBlock.allocated = false;
                        metaFreeBlocks.add(metaBlock.blockId);
                        System.err.println("Failed to add metadata page to meta block");
                        return null;
                    }
                    // 4.3 计算新SSTable的键范围（论文3.C：SSTable键范围=所有KV页键范围的并集）
                    String[] newSstKeyRangeParts = currentGroup.get(currentGroup.size()-(int) (Constants.BLOCK_SIZE / Constants.PAGE_SIZE)).second.split("\\|"); // 注意转义竖线（|在正则中需转义）
                    String newSstMinKey = newSstKeyRangeParts[0].trim(); // 去除空格，确保键比较准确
                    String newSstMaxKey = next.second.split("\\|")[0].trim();
//                    String numericPart = newSstMaxKey.substring(4); // 提取"user"之后的部分
//                    long numericValue = Long.parseLong(numericPart);
//                    if (numericValue > 0) { // 确保不会出现负数
//                        newSstMaxKey = "user" + (numericValue - 1);
//                    }
                    newSst.keyRange = new Pair<>(newSstMinKey, newSstMaxKey); // 赋值SSTable键范围
                    newSst.metadataPagePpa = newMetaPage.ppa; // 关联MetaPage的Flash物理地址（PPA）
                    try {
                        savePhysicalBlockToFile(metaBlock);
                        saveMetaPageToFile(metaBlock);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        //  e.printStackTrace();
                    }
                    newSSTables.add(newSst);
                    currentGroup.clear();
                    for (Pair<String ,String> entry:sharedPages){
                        currentGroup.add(entry);
                    }
                    currentCount = 0;
                }
                else {

                    SSTable newSst = new SSTable(nextSstId++, targetLevel); // nextSstId为全局自增ID，确保唯一性
                    PhysicalBlock metaBlock = allocateMetaBlock();
                    String metaPpa = generatePPA(metaBlock.blockId); // 复用原有PPA生成逻辑
                    newSst.metadataPagePpa=metaPpa;
                    PhysicalPage newMetaPage = new PhysicalPage(metaPpa);
                    for ( Pair<String, String> kvEntry : currentGroup) {
                        newMetaPage.data.add(kvEntry); // 将KV页条目加入新MetaPage
                    }
                    int metaPageNo = Integer.parseInt(metaPpa.split("_")[1]);
                    if (metaBlock.addPage(metaPageNo, newMetaPage)) {
                        newSst.metadataPage = newMetaPage;
                        stats.totalFlashWrites += Constants.PAGE_SIZE; // 统计元数据页写入
                        metaBlock.sstables.add(newSst.sstId);
                        try {
                            savePhysicalBlockToFile(metaBlock); // 持久化元数据区块变更
                        } catch (IOException e) {
                            System.err.println("Failed to persist meta block after adding metadata page: " + e.getMessage());
                        }
                    } else {
                        // 元数据区块添加失败时，释放块并返回错误
                        metaBlock.allocated = false;
                        metaFreeBlocks.add(metaBlock.blockId);
                        System.err.println("Failed to add metadata page to meta block");
                        return null;
                    }
                    String[] newSstKeyRangeParts = currentGroup.get(0).second.split("\\|"); // 注意转义竖线（|在正则中需转义）
                    String newSstMinKey = newSstKeyRangeParts[0].trim();
                    Collections.sort(currentGroup, new Comparator<Pair<String, String>>() {
                        @Override
                        public int compare(Pair<String, String> entry1, Pair<String, String> entry2) {
                            String keyMax1 = extractKeyMax(entry1.second);
                            String keyMax2 = extractKeyMax(entry2.second);
                            return keyMax2.compareTo(keyMax1); // 降序
                        }
                        private String extractKeyMax(String keyRange) {
                            return (keyRange != null && keyRange.contains("|")) ?
                                    keyRange.split("\\|")[1].trim() : "";
                        }
                    });
                    String[] KeyRangeParts = currentGroup.get(0).second.split("\\|");
                    String newSstMaxKey = KeyRangeParts[1].trim();

                    newSst.keyRange = new Pair<>(newSstMinKey, newSstMaxKey); // 赋值SSTable键范围
                    newSst.metadataPagePpa = newMetaPage.ppa; // 关联MetaPage的Flash物理地址（PPA）
                    try {
                        savePhysicalBlockToFile(metaBlock);
                        saveMetaPageToFile(metaBlock);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        //  e.printStackTrace();
                    }
                    newSSTables.add(newSst);
                    // 清空 currentGroup，继续（循环会结束）
                    currentGroup.clear();
                    currentCount = 0;
                }
            }
        }
        return newSSTables;
    }

    private SSTable createSSTable(List<Pair<String, String>> kvEntries, int targetLevel, String nextMinKey, boolean isLast) {
        SSTable newSst = new SSTable(nextSstId++, targetLevel);
        PhysicalBlock metaBlock = allocateMetaBlock();
        if (metaBlock == null) {
            return null;
        }

        String metaPpa = generatePPA(metaBlock.blockId);
        newSst.metadataPagePpa = metaPpa;
        PhysicalPage newMetaPage = new PhysicalPage(metaPpa);

        // 9. 添加后立即从kvEntries移除元素，减少内存占用
        Iterator<Pair<String, String>> iterator = kvEntries.iterator();
        while (iterator.hasNext()) {
            newMetaPage.data.add(iterator.next());
            iterator.remove(); // 移除已处理的元素，释放内存
        }

        int metaPageNo = Integer.parseInt(metaPpa.split("_")[1]);
        if (!metaBlock.addPage(metaPageNo, newMetaPage)) {
            metaBlock.allocated = false;
            metaFreeBlocks.add(metaBlock.blockId);
            return null;
        }

        newSst.metadataPage = newMetaPage;
        stats.totalFlashWrites += Constants.PAGE_SIZE;
        metaBlock.sstables.add(newSst.sstId);

        // 计算键范围
        String newSstMinKey;
        String newSstMaxKey;

        if (!isLast) {
            // 非最后一组：最小键取自当前组第0个元素（原代码currentGroup.size()-128可能越界，修正为0）
            String[] firstKeyParts = kvEntries.isEmpty() ? new String[]{"", ""} : kvEntries.get(0).second.split("\\|", 2);
            newSstMinKey = firstKeyParts[0].trim();
            newSstMaxKey = nextMinKey;
        } else {
            // 最后一组：最小键取自第一个元素
            String[] firstKeyParts = kvEntries.get(0).second.split("\\|", 2);
            newSstMinKey = firstKeyParts[0].trim();

            // 排序时使用缓存的键范围，减少split调用
            kvEntries.sort((e1, e2) -> {
                String max1 = e1.second.split("\\|", 2)[1].trim();
                String max2 = e2.second.split("\\|", 2)[1].trim();
                return max2.compareTo(max1);
            });
            String[] maxKeyParts = kvEntries.get(0).second.split("\\|", 2);
            newSstMaxKey = maxKeyParts[1].trim();
        }

        newSst.keyRange = new Pair<>(newSstMinKey, newSstMaxKey);
        newSst.metadataPagePpa = newMetaPage.ppa;

        // 持久化操作
        try {
            savePhysicalBlockToFile(metaBlock);
            saveMetaPageToFile(metaBlock);
        } catch (IOException e) {
            System.err.println("Failed to persist blocks: " + e.getMessage());
            metaBlock.allocated = false;
            metaFreeBlocks.add(metaBlock.blockId);
            return null;
        }

        return newSst;
    }

    // 同步修改runCompaction中的目标层级列表处理（若有类似Stream逻辑）
    private void newRunCompaction(int sourceLevel, List<SSTable> overlappingSsts) {
        stats.compactionCount++;
        int targetLevel = sourceLevel + 1;

        // 1. 用for循环获取目标层级有效SSTable(ppa，key_min|key_max)
        // List<Pair<String, String>> allKvPageEntries=new ArrayList<>();

        // for(SSTable sst:overlappingSsts){
        //     List<Pair<String,String>> ppaAndRange=sst.metadataPage.data;
        //     for(Pair<String,String> onePage:ppaAndRange){
        //         allKvPageEntries.add(onePage);
        //     }
        // }
        //TODO kventries去重
        Map<String, Pair<String, String>> uniqueEntries = new LinkedHashMap<>();
        for (SSTable sst : overlappingSsts) {
            List<Pair<String, String>> ppaAndRange = sst.metadataPage.data;
            for (Pair<String, String> onePage : ppaAndRange) {
                String key = onePage.first; // 以键作为去重依据
                if (!uniqueEntries.containsKey(key)) {
                    uniqueEntries.put(key, onePage);
                }
            }
        }
        List<Pair<String, String>> allKvPageEntries = new ArrayList<>(uniqueEntries.values());

        // 步骤5：加入新SSTable并按键范围排序（后续逻辑不变）
        sortAllKvPageEntriesByMinKeyAsc(allKvPageEntries);
        List<SSTable> newSsts = newnewSplitIntoNonOverlappingSsts(allKvPageEntries, targetLevel);

        // 2. 标记原 SSTable 为无效（业务逻辑：原 SSTable 不再参与查询）
        for(SSTable sst:overlappingSsts){
            //     System.out.println("delete sstable: "+sst.sstId);
            markSSTInvalid(sst);
        }
//        for(SSTable sst:newSsts){
//            System.out.println("add sstable: "+sst.sstId);
//        }
        // 更新目标层级列表到lsmLevels
        // newSsts.sort(Comparator.comparing(s -> s.keyRange.first));
        // lsmLevels.put(targetLevel, newSsts);

        //new
        // 合并新SSTable到目标层级（新老数据一起处理）
        List<SSTable> temp=lsmLevels.getOrDefault(targetLevel, new ArrayList<>());


        for(SSTable sst:newSsts){
            temp.add(sst);
        }
        temp.sort(Comparator.comparing(s -> s.keyRange.first));
        lsmLevels.put(targetLevel, temp);
        // 5. 对合并后的所有SSTable按keyRange.first（keyMin）升序排序


        // 其余持久化等逻辑保持不变...
        try {
            for(SSTable sst:newSsts){
                saveSSTableToFile(sst);
            }
        } catch (IOException e) {
            // System.err.printf("持久化新 SST[%d] 失败：%s%n", sst.sstId, e.getMessage());
        }
    }

    /**
     * L1+ 单个迁移 SSTable 到下一层（创建新 SSTable，删除原 SSTable）
     */
    private void runCompaction(int sourceLevel, SSTable victimSst) {
        stats.compactionCount++;
        int targetLevel = sourceLevel + 1; // 目标层级 = 源层级 + 1
        List<SSTable> targetSsts = lsmLevels.computeIfAbsent(targetLevel, k -> new ArrayList<>());

        // 1. 【修复】创建新 SSTable：层级设为目标层级（原代码固定为 1，此处改为动态目标层级）
        SSTable newSst = new SSTable(nextSstId++, targetLevel); // 新 ID + 动态目标层级
        newSst.keyRange = victimSst.keyRange;          // 复制键范围（与原一致）
        newSst.kvpairSize = victimSst.kvpairSize;      // 复制 KV 数量
        newSst.metadataPagePpa = victimSst.metadataPagePpa; // 复制元数据页地址
        newSst.metadataPage=victimSst.metadataPage;
        // （补充其他需复制的属性，确保新 SSTable 与原完全一致）

        // 2. 标记原 SSTable 为无效（业务逻辑：原 SSTable 不再参与查询）
        markSSTInvalid(victimSst);

        // 3. 将新 SSTable 加入目标层级，并按键范围排序（L1+ 需保持有序，便于查询优化）
        targetSsts.remove(victimSst);
        targetSsts.add(newSst);
        targetSsts.sort(Comparator.comparing(s -> s.keyRange.first));

        // 4. 更新目标层级列表到 LSM 树
        lsmLevels.put(targetLevel, targetSsts);

        System.out.printf("层级[%d] SST[%d] 迁移到层级[%d]，新 SST ID：%d%n",
                sourceLevel, victimSst.sstId, targetLevel, newSst.sstId);

        try {
            saveSSTableToFile(newSst);
        } catch (IOException e) {
            System.err.printf("持久化新 SST[%d] 失败：%s%n", newSst.sstId, e.getMessage());
        }    // （可选）若需持久化新 SSTable 元数据，可在此补充保存逻辑

    }
    /**
     * 传统压缩（原有逻辑保留，补充标记无效后持久化）
     */
    private void conventionalCompaction(int level, SSTable victimSst, List<SSTable> overlappingSsts) {
        int nextLevel = level + 1;

        // 1. 合并 KV 对并去重
        Map<String, String> allKvs = new HashMap<>();
        // 读取受害者 SST
        for (PhysicalPage page : victimSst.kvPages) {
            for (Pair<String, String> kv : page.data) {
                allKvs.put(kv.first, kv.second);
            }
        }
        // 读取重叠 SST
        for (SSTable sst : overlappingSsts) {
            for (PhysicalPage page : sst.kvPages) {
                for (Pair<String, String> kv : page.data) {
                    allKvs.put(kv.first, kv.second);
                }
            }
        }

        // 2. 排序并写入新 SSTable
        List<Pair<String, String>> sortedKvs = allKvs.entrySet().stream()
                .map(entry -> new Pair<>(entry.getKey(), entry.getValue()))
                .sorted(Comparator.comparing(p -> p.first))
                .collect(Collectors.toList());
        writeMemtableToSSTable(sortedKvs, nextLevel);

        // 3. 标记旧 SST 为无效
        markSSTInvalid(victimSst);
        for (SSTable sst : overlappingSsts) {
            markSSTInvalid(sst);
        }

        // 4. 检查下一层级压缩
        checkLevelCompaction(nextLevel);
    }

    /**
     * 标记 SST 为无效（改造：标记后持久化）
     */
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

        // 3. 更新页引用计数和有效性
        for (PhysicalPage page : sst.kvPages) {
            page.refCount--;
            if (page.refCount <= 0) {
                page.valid = false;
            }
        }
        if (sst.metadataPage != null) {
            sst.metadataPage.refCount--;
            if (sst.metadataPage.refCount <= 0) {
                sst.metadataPage.valid = false;
            }
        }

        // 4. 持久化变更（物理块元数据、键范围树）并删除无效 SST 文件
        try {
            // 找到 SST 关联的物理块并持久化
            for (PhysicalBlock block : physicalBlocks.values()) {
                if (block.sstables.contains(sst.sstId)) {
                    block.sstables.remove(sst.sstId);
                    savePhysicalBlockToFile(block);
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
     * 工具方法：更新所有SST中旧页的引用为新页
     */
    private void updateSSTPageReference(String oldPpa, PhysicalPage newPage) {
        for (List<SSTable> ssts : lsmLevels.values()) {
            for (SSTable sst : ssts) {
                // 更新KV页引用
                for (int i = 0; i < sst.kvPages.size(); i++) {
                    if (sst.kvPages.get(i).ppa.equals(oldPpa)) {
                        sst.kvPages.set(i, newPage);
                    }
                }
                // 更新元数据页中的KV页地址
                if (sst.metadataPage != null) {
                    for (Pair<String, String> metaEntry : sst.metadataPage.data) {
                        if (metaEntry.first.equals(oldPpa)) {
                            metaEntry.first = newPage.ppa;
                            break;
                        }
                    }
                }
            }
        }
    }

    /**
     * 删除指定 SST 的明文文件（如果存在）。
     */
    private void deleteSstFile(long sstId) {
        try {
            Files.createDirectories(Paths.get(Constants.SST_DIR));
            Path sstPath = Paths.get(Constants.SST_DIR + "sst_" + sstId + ".sst");
            Files.deleteIfExists(sstPath);
        } catch (IOException e) {
            System.err.println("Failed to delete SST file sst_" + sstId + ".sst: " + e.getMessage());
        }
    }

    /**
     * 执行垃圾回收（改造：复用待回收块空间，不依赖allocateBlock）
     */
    private void runGarbageCollection() {
        stats.gcCount++;
        List<PhysicalBlock> gcBlocks = new ArrayList<>();

        // 1. 筛选需回收的块（无效页比例超阈值，且优先选同层级块）
        for (PhysicalBlock block : physicalBlocks.values()) {
            if (block.allocated && block.getInvalidRatio() >= Constants.GC_THRESHOLD) {
                gcBlocks.add(block);
                if (gcBlocks.size() >= 2) break; // 每次回收2个块（控制GC开销）
            }
        }
        if (gcBlocks.isEmpty()) {
            System.out.println("No blocks need GC (invalid ratio < " + Constants.GC_THRESHOLD + ")");
            return;
        }
        System.out.println("Start GC: process " + gcBlocks.size() + " blocks (invalid ratio >= " + Constants.GC_THRESHOLD + ")");

        // 2. 迁移有效页（核心优化：优先复用待回收块的空闲空间）
        for (PhysicalBlock oldBlock : gcBlocks) {
            // 2.1 收集当前待回收块的有效页和空闲页位置
            List<PhysicalPage> validPages = oldBlock.pages.stream()
                    .filter(page -> page != null && page.valid)
                    .collect(Collectors.toList());
            List<Integer> freePageNosInOldBlock = new ArrayList<>(); // 待回收块内的空闲页编号
            for (int i = 0; i < oldBlock.pages.size(); i++) {
                if (oldBlock.pages.get(i) == null || !oldBlock.pages.get(i).valid) {
                    freePageNosInOldBlock.add(i);
                }
            }

            // 2.2 优先用待回收块的空闲页迁移有效页
            Map<PhysicalPage, Integer> validPageToNewNo = new HashMap<>(); // 有效页→新页号映射
            for (int i = 0; i < validPages.size() && i < freePageNosInOldBlock.size(); i++) {
                validPageToNewNo.put(validPages.get(i), freePageNosInOldBlock.get(i));
            }

            // 2.3 若待回收块空间不足，从其他空闲块找空间（此时空闲块已提前释放）
            List<PhysicalBlock> tempFreeBlocks = new ArrayList<>();
            if (validPageToNewNo.size() < validPages.size()) {
                Iterator<Long> freeIter = freeBlocks.iterator();
                while (freeIter.hasNext() && tempFreeBlocks.size() < 2) { // 最多临时用2个空闲块
                    long freeBlockId = freeIter.next();
                    PhysicalBlock freeBlock = physicalBlocks.get(freeBlockId);
                    if (freeBlock.level == oldBlock.level) { // 优先同层级空闲块
                        tempFreeBlocks.add(freeBlock);
                        freeIter.remove(); // 临时占用，避免被其他线程分配
                    }
                }
            }

            // 2.4 执行有效页迁移
            for (PhysicalPage validPage : validPages) {
                PhysicalBlock targetBlock;
                int targetPageNo;

                // 情况1：复用待回收块的空闲页
                if (validPageToNewNo.containsKey(validPage)) {
                    targetBlock = oldBlock;
                    targetPageNo = validPageToNewNo.get(validPage);
                }
                // 情况2：用临时空闲块的空闲页
                else {
                    // 找第一个有空闲页的临时块
                    targetBlock = tempFreeBlocks.stream()
                            .filter(b -> !b.isFull())
                            .findFirst()
                            .orElseThrow(() -> new RuntimeException("No temp free block for GC migration"));
                    // 找临时块的第一个空闲页
                    targetPageNo = -1;
                    for (int i = 0; i < targetBlock.pages.size(); i++) {
                        if (targetBlock.pages.get(i) == null) {
                            targetPageNo = i;
                            break;
                        }
                    }
                }

                // 创建新页并迁移数据（复用目标块空间，无需allocateBlock）
                String newPpa = targetBlock.blockId + "_" + targetPageNo;
                PhysicalPage newPage = new PhysicalPage(newPpa);
                newPage.data = validPage.data;
                newPage.keyRange = validPage.keyRange;
                newPage.refCount = validPage.refCount;

                // 将新页加入目标块
                if (targetBlock.addPage(targetPageNo, newPage)) {
                    // 更新所有关联SST的页引用（KV页+元数据页）
                    updateSSTPageReference(validPage.ppa, newPage);
                    stats.totalFlashWrites += Constants.PAGE_SIZE;
                    System.out.println("GC migrated page: " + validPage.ppa + " → " + newPpa);
                }
            }

            // 2.5 归还临时占用的空闲块（迁移完成后释放）
            for (PhysicalBlock tempBlock : tempFreeBlocks) {
                tempBlock.allocated = false;
                freeBlocks.add(tempBlock.blockId);
                try {
                    savePhysicalBlockToFile(tempBlock);
                } catch (IOException e) {
                    System.err.println("Failed to persist temp free block after GC: " + e.getMessage());
                }
            }

            // 3. 擦除旧块（迁移完成后释放为空闲块）
            eraseBlock(oldBlock);
        }

        // 4. GC后持久化所有变更
        try {
            for (PhysicalBlock block : physicalBlocks.values()) {
                if (block.allocated) {
                    savePhysicalBlockToFile(block);
                }
            }
            // 重新持久化所有受影响的SSTable
            for (List<SSTable> ssts : lsmLevels.values()) {
                for (SSTable sst : ssts) {
                    saveSSTableToFile(sst);
                }
            }
            //saveKeyRangeTreeToDisk();
        } catch (IOException e) {
            System.err.println("Failed to persist after GC: " + e.getMessage());
        }
        System.out.println("GC completed: free blocks now " + freeBlocks.size() + " (ratio " + String.format("%.1f", getFreeBlockRatio()) + "%)");
    }
    /**
     * 擦除物理块（改造：擦除后持久化）
     */
    private void eraseBlock(PhysicalBlock block) {
        // 重置块状态
        for (PhysicalPage page : block.pages) {
            if (page != null) {
                page.valid = false;
                page.refCount = 0;
            }
        }
        block.pages.clear();
        block.pages = new ArrayList<>(Collections.nCopies((int) (Constants.BLOCK_SIZE / Constants.PAGE_SIZE), null));
        block.level = -1;
        block.sstables.clear();
        block.allocated = false;

        // 加入空闲列表
        freeBlocks.add(block.blockId);

        // 持久化擦除后的状态
        try {
            savePhysicalBlockToFile(block);
        } catch (IOException e) {
            System.err.println("Failed to persist after erasing block: " + e.getMessage());
        }
    }


    /**
     * 【新增】统一的 Memtable 刷盘方法，支持“强制刷盘”和“按大小检查刷盘”
     * @param forceFlush true=强制刷盘（不管当前大小），false=仅当大小超限时刷盘
     */
    private void flushMemtableIfNeeded(boolean forceFlush) {
        synchronized (this) {
            int currentSize = memtable.stream()
                    .mapToInt(this::calculateKvSize)
                    .sum();

            // 触发条件：要么强制刷盘，要么大小超上限
            if (forceFlush || currentSize >= Constants.MAX_MEMTABLE_SIZE) {
                if (memtable.isEmpty()) {
                    System.out.println("Memtable 为空，无需刷盘");
                    return;
                }

                System.out.println("触发 Memtable 刷盘（当前大小：" + currentSize + "字节，上限：" + Constants.MAX_MEMTABLE_SIZE + "字节）");
                // 转为不可变 Memtable 并清空当前 Memtable
                immutableMemtables.add(new ArrayList<>(memtable));
                memtable.clear();

                // 清空持久化的 Memtable 文件（原有逻辑保留）
                try {
                    new FileWriter(Constants.MEMTABLE_FILE, false).close();
                } catch (IOException e) {
                    System.err.println("Failed to clear persisted memtable: " + e.getMessage());
                }

                // 刷盘所有不可变 Memtable（原有逻辑保留）
                while (!immutableMemtables.isEmpty()) {
                    List<Pair<String, String>> immMem = immutableMemtables.poll();
                    writeMemtableToSSTable(immMem, 0);
                    checkLevelCompaction(0);
                }
            }
        }
    }

    /**
     * 【修改】原有 checkMemtableFull 方法，复用 flushMemtableIfNeeded 避免代码重复
     */
    private void checkMemtableFull() {
        // 仅按大小检查刷盘，不强制（调用统一刷盘方法）
        flushMemtableIfNeeded(false);
    }

    /**
     * 【保留/统一】计算单个 KV 对的字节大小（key + value 的字节长度总和）
     */
    private int calculateKvSize(Pair<String, String> kv) {
        return kv.first.getBytes(StandardCharsets.UTF_8).length
                + kv.second.getBytes(StandardCharsets.UTF_8).length;
    }
    // ==================== KV 操作接口（原有逻辑保留）====================
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
                flushMemtableIfNeeded(true); // 传 true 表示“强制刷盘”
            }

            // 3. 现在空间足够，写入 Memtable（原有逻辑保留，先删旧键避免重复）
            memtable.removeIf(kv -> kv.first.equals(key));
            memtable.add(new Pair<>(key, value));
        }


        synchronized (memtable) {
            // 先删除旧键（避免重复）
            memtable.removeIf(kv -> kv.first.equals(key));
            memtable.add(new Pair<>(key, value));
        }
        stats.writeCount++;
        checkMemtableFull();
        // 更新写入放大
        stats.writeAmplification = stats.totalFlashWrites > 0
                ? (double) stats.totalFlashWrites / (stats.writeCount * (key.getBytes(StandardCharsets.UTF_8).length +
                value.getBytes(StandardCharsets.UTF_8).length))
                : 0;
    }


    public String get(String key) {

        stats.readCount++;
        int flashAccess = 0;

        // 1. 查活跃 Memtable
        synchronized (memtable) {
            for (Pair<String, String> kv : memtable) {
                if (kv.first.equals(key)) {
                    updateReadStats(flashAccess);
                    return kv.second;
                }
            }
        }

        // 2. 查不可变 Memtable
        for (List<Pair<String, String>> immMem : immutableMemtables) {
            for (Pair<String, String> kv : immMem) {
                if (kv.first.equals(key)) {
                    updateReadStats(flashAccess);
                    return kv.second;
                }
            }
        }

        // 3. 查 LSM 树（SSTable）
        for (int i=0;i<lsmLevels.size();i++) {
          //   System.out.println("in level:"+i);
            List<SSTable> levelSsts =lsmLevels.get(i);
            if (levelSsts == null || levelSsts.isEmpty()) {
                continue; // 跳过空层级
            }

            // 2. 遍历当前层级的SSTable（已按键范围起始值升序排列，利用不重叠特性优化）
            boolean levelHasPotential = false; // 标记当前层级是否可能包含目标键
            for (int j=0;j<levelSsts.size();j++) {
                SSTable sst = levelSsts.get(j);
               // System.out.println("level " + i + " 's  sstable!");
                // 2.1 目标键 < 当前SSTable的起始键：后续SSTable起始键更大，直接跳出该层级
                if (key.compareTo(sst.keyRange.first) < 0) {
                    //  System.out.println("skip this level");
                    break;
                }
                // 2.2 目标键 > 当前SSTable的结束键：继续检查下一个SSTable
//                if (key.compareTo(sst.keyRange.second) >= 0) {
//                    // System.out.println("skip this sst"+sst.sstId);
//                    continue;
//                }
                if((key.compareTo(sst.keyRange.first) >= 0 && key.compareTo(sst.keyRange.second) <0)||(key.compareTo(sst.keyRange.first) >= 0 && key.compareTo(sst.keyRange.second) <=0 && j==levelSsts.size()-1)){
                    // System.out.println("真的开始读文件了");
                    // 2.3 目标键在当前SSTable的键范围内：开始查询该SSTable
                   // System.out.println("hit this sst:"+sst.sstId);
                    levelHasPotential = true;
                    flashAccess++; // 1. 访问SSTable的Meta Page（闪存访问：读取SSTable元数据）
                    stats.totalFlashReads++;

                    // -------------------------- 核心修改1：解析Meta Page PPA，定位【Meta Block元数据文件】 --------------------------
                    // Meta Page PPA格式："块ID_页偏移"（如"0_0" → 块ID=0，页偏移忽略）
                    //  System.out.println("sst中读出来meta pag ppa ："+sst.metadataPagePpa);
                    //  System.out.println("metappa:"+sst.metadataPagePpa);
                    String[] metaPpaParts = sst.metadataPagePpa.split("_");
                    if (metaPpaParts.length < 1) {
                        System.err.println("Invalid Meta Page PPA: " + sst.metadataPagePpa + " (SST ID: " + sst.sstId + ")");
                        continue;
                    }
                    String metaBlockId = metaPpaParts[0]; // 提取块ID（决定Meta Block元数据文件名）
                    // 构造Meta Block元数据文件路径（如：Constants.META_BLOCK_META_DIR/0.txt）
                    // System.out.println("读出来meta文件 ："+Constants.META_BLOCK_META_DIR + metaBlockId + ".txt");
                    File metaBlockMetaFile = new File(Constants.META_BLOCK_META_DIR + metaBlockId + ".txt");
                    if (!metaBlockMetaFile.exists()) {
                        System.err.println("Meta Block元数据文件不存在: " + metaBlockMetaFile.getAbsolutePath());
                        continue;
                    }
                    //  System.out.println("read this mate file:"+Constants.META_BLOCK_META_DIR + metaBlockId + ".txt");
                    // -------------------------- 核心修改2：读取【Meta Block元数据文件】，解析KV Page映射关系 --------------------------
                    // 存储：KV Page PPA → 键范围（从Meta Block元数据文件中提取）
                    // KV Page PPA格式："数据块ID_页编号"（如"0_22" → 数据块ID=0，页编号=22）
                    Map<String, Pair<String, String>> kvPageRangeMap = new HashMap<>();
                    try (BufferedReader metaReader = new BufferedReader(
                            new InputStreamReader(new FileInputStream(metaBlockMetaFile), StandardCharsets.UTF_8))) {
                        String metaLine;
                        // 解析文件中"KV Page映射行"（格式：0_22|userXXX||userYYY）
                        while ((metaLine = metaReader.readLine()) != null) {
                            metaLine = metaLine.trim();
                            if (metaLine.isEmpty()) continue;

                            // 仅处理KV Page映射行（特征：包含"|"，且格式为"数据块ID_页编号|起始键||结束键"）
                            if (metaLine.contains("|")) {
                                // 按"|"分割：第0段=KV Page PPA，第1段=起始键，第3段=结束键（兼容"||"分隔）
                                String[] kvPageParts = metaLine.split("\\|");
                                if (kvPageParts.length < 4) { // 确保格式正确（如：0_22|key1||key2 → 分割后长度=4）
                                    //           System.err.println("无效的KV Page映射格式: " + metaLine + " (文件: " + metaBlockMetaFile.getName() + ")");
                                    continue;
                                }
                                String kvPagePpa = kvPageParts[0].trim();       // KV Page PPA（如"0_22"）
                                String pageKeyStart = kvPageParts[1].trim();    // 页起始键（如"user2435493937695570810"）
                                String pageKeyEnd = kvPageParts[3].trim();      // 页结束键（如"user2502149056439609611"）

                                // 过滤空键范围（避免无效数据）
                                if (!pageKeyStart.isEmpty() && !pageKeyEnd.isEmpty()) {
                                    kvPageRangeMap.put(kvPagePpa, new Pair<>(pageKeyStart, pageKeyEnd));
                                }
                            }
                        }
                        //    System.out.println("meta文件中有"+kvPageRangeMap.size()+"个页");
                    } catch (IOException e) {
                        System.err.println("读取Meta Block元数据文件失败: " + metaBlockMetaFile.getAbsolutePath() + " - " + e.getMessage());
                        continue;
                    }

                    // -------------------------- 核心修改3：筛选目标KV Page，定位【实际数据Block文件】 --------------------------
                    List<String> targetKvPagePpas = new ArrayList<>(); // 匹配到的目标KV Page PPA
                    Pair<String, String> targetPageRange = null; // 目标KV Page的键范围
                    for (Map.Entry<String, Pair<String, String>> entry : kvPageRangeMap.entrySet()) {
                        String kvPagePpa = entry.getKey();
                        Pair<String, String> pageRange = entry.getValue();

                        if(key.compareTo(pageRange.first)>=0 && key.compareTo(pageRange.second)<=0){
                            flashAccess++; // 1. 访问SSTable的Meta Page（闪存访问：读取SSTable元数据）
                            stats.totalFlashReads++;
                           // System.out.println("physical ppa:"+kvPagePpa+"hit!");
                            // 3.3 目标键在当前Page键范围内：记录目标Page信息
                            targetKvPagePpas.add(kvPagePpa);
                            // targetPageRange = pageRange;
                            //break;
                        }

                    }

                    // 无匹配的KV Page：跳过当前SSTable
                    if (targetKvPagePpas == null) {
                        System.out.println("当前SSTable中无包含目标键的KV Page");
                        continue;
                    }

                    // -------------------------- 核心修改4：解析目标KV Page PPA，定位【实际数据Block文件】 --------------------------
                    // 目标KV Page PPA格式："数据块ID_页编号"（如"0_22" → 数据块ID=0，页编号=22）
                    for(String targetKvPagePpa:targetKvPagePpas){
                        String[] dataPpaParts = targetKvPagePpa.split("_");
                        if (dataPpaParts.length < 2) {
                            // System.err.println("无效的KV Page PPA: " + targetKvPagePpa + " (SST ID: " + sst.sstId + ")");
                            // continue;
                        }
                        String dataBlockId = dataPpaParts[0]; // 数据块ID（决定实际数据文件名）
                        int targetPageNo = Integer.parseInt(dataPpaParts[1]); // 目标页编号（如22）
                        // 目标页文件路径：块目录/页编号.txt（如 ./block_meta/0/22.txt）
                        File targetPageFile = new File(Constants.BLOCK_META_DIR + dataBlockId + "/" + targetPageNo + ".txt");
                        if (!targetPageFile.exists()) {
                            System.err.println("目标页文件不存在: " + targetPageFile.getAbsolutePath());
                            continue;
                        }
                        // -------------------------- 核心修改5：读取【实际数据Block文件】，提取目标KV对 --------------------------
                        flashAccess++; // 2. 访问实际数据Block文件（闪存访问：读取KV Page数据）
                        stats.totalFlashReads++;
                        // 调用工具方法读取目标页的KV对（需确保readKvFromDataBlock方法适配新的文件格式）
                        String targetValue = readKvFromPageFile(targetPageFile, key);
                        if (targetValue != null) {
                            updateReadStats(flashAccess);
                            return targetValue;
                        }
                    }
//                  //  break;
                }
            }
            // 4. 若当前层级已找到潜在匹配的 SSTable，但未找到键（可能被覆盖），仍需检查更低层级
            // （注：LSM 树中同一键可能在多层级存在，需确认所有层级）
        }
        // 4. 未找到
        updateReadStats(flashAccess);
        System.out.println("KVSSD未找到键：" + key);
        return null;
    }
    /**
     * 从单独的页文件（如0.txt）中读取目标键对应的value
     */
    private String readKvFromPageFile(File pageFile, String targetKey) {
        boolean parsingKvPairs = false;
        // try-with-resources 语法仍保留（自动关闭流，避免资源泄漏）
        try (BufferedReader pageReader = new BufferedReader(
                new InputStreamReader(new FileInputStream(pageFile), StandardCharsets.UTF_8))) {

            String line;
            while ((line = pageReader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }

                // 标记：开始解析KV对
                if (line.equals("KV_PAIRS_START")) {
                    parsingKvPairs = true;
                    continue;
                }
                // 标记：结束解析KV对（跳出循环，避免无效读取）
                if (line.equals("KV_PAIRS_END")) {
                    parsingKvPairs = false;
                    break;
                }

                // 仅在KV对解析阶段处理数据
                if (parsingKvPairs) {
                    // 1. 分割键值对（限制分割1次，避免value含"->"导致错误）
                    String[] kvParts = line.split("->", 2);
                    if (kvParts.length != 2) {
                        System.err.printf("页文件[%s]存在无效KV格式：%s%n", pageFile.getName(), line);
                        continue; // 跳过无效行，继续解析后续KV
                    }

                    // 2. 还原保存时转义的分隔符（与 savePhysicalBlockToFile 逻辑对应）
                    String key = kvParts[0].trim().replace("-#->", "->");
                    String value = kvParts[1].trim().replace("-#->", "->");

                    // 3. 匹配目标键，找到后直接返回（提前终止，减少无效遍历）
                    if (key.equals(targetKey)) {
                        return value;
                    }
                }
            }

        } catch (FileNotFoundException e) {
            // 捕获“文件不存在”异常（更具体的异常类型，便于定位问题）
            System.err.printf("目标页文件不存在：%s，异常信息：%s%n", pageFile.getAbsolutePath(), e.getMessage());
        } catch (IOException e) {
            // 捕获所有其他IO异常（如读取出错、流关闭失败等）
            System.err.printf("读取页文件[%s]失败，异常信息：%s%n", pageFile.getAbsolutePath(), e.getMessage());
        } catch (Exception e) {
            // 捕获非IO异常（如编码错误、数据格式异常等，增强鲁棒性）
            System.err.printf("处理页文件[%s]时发生未知错误，异常信息：%s%n", pageFile.getAbsolutePath(), e.getMessage());
        }

        // 以下情况均返回null：
        // 1. 正常遍历完所有KV对，未找到目标键
        // 2. 发生任何异常（文件不存在、读取出错等）
        return null;
    }
    /**
     * 更新读取统计
     */
    private void updateReadStats(int flashAccess) {
        if (flashAccess == 0) {
            stats.read0Flash++;
        } else if (flashAccess == 1) {
            stats.read1Flash++;
        } else if (flashAccess == 2) {
            stats.read2Flash++;
        } else if (flashAccess == 3) {
            stats.read3Flash++;
        } else if (flashAccess == 4) {
            stats.read4Flash++;
        } else if (flashAccess == 5) {
            stats.read5Flash++;
        } else if (flashAccess == 6) {
            stats.read6Flash++;
        } else if (flashAccess == 7) {
            stats.read7Flash++;
        } else if (flashAccess == 8) {
            stats.read8Flash++;
        }  else {
            stats.readMoreFlash++;
        }
    }

    /**
     * 获取性能统计
     */
    public Stats getStats() {
        Stats copy = new Stats();
        copy.writeCount = stats.writeCount;
        copy.readCount = stats.readCount;
        copy.gcCount = stats.gcCount;
        copy.compactionCount = stats.compactionCount;
        copy.writeAmplification = stats.writeAmplification;
        copy.totalFlashWrites = stats.totalFlashWrites;
        copy.totalFlashReads = stats.totalFlashReads;
        copy.read0Flash = stats.read0Flash;
        copy.read1Flash = stats.read1Flash;
        copy.read2Flash = stats.read2Flash;
        copy.read3Flash = stats.read3Flash;
        copy.read4Flash = stats.read4Flash;
        copy.read5Flash = stats.read5Flash;
        copy.read6Flash = stats.read6Flash;
        copy.read7Flash = stats.read7Flash;
        copy.read8Flash = stats.read8Flash;
        copy.readMoreFlash = stats.readMoreFlash;
        return copy;
    }

    /**
     * 初始化（空实现，构造函数已处理）
     */
    public void init() {
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

    /**
     * 清理（核心：强制刷盘所有内存数据）
     */
    public void cleanup() {
        try {
            // === 1. 先保存活跃 memtable 到单独的文件 ===
            synchronized (memtable) {
                if (!memtable.isEmpty()) {
                    saveMemtableToDisk();
                    // 转换为 immutable，准备刷到 SSTable
                    //immutableMemtables.add(new ArrayList<>(memtable));
                    memtable.clear();
                }
            }

            // === 2. 刷 immutable memtables 成 SSTable ===
            while (!immutableMemtables.isEmpty()) {
                List<Pair<String, String>> immMem = immutableMemtables.poll();
                writeMemtableToSSTable(immMem, 0);   // 正常 LSM Tree 写入
                checkLevelCompaction(0);             // 层级合并
            }

            // === 3. 持久化最终状态（键范围树、物理块） ===
            // saveKeyRangeTreeToDisk();
            saveLsmLevelsToDisk();
            for (PhysicalBlock block : physicalBlocks.values()) {
                savePhysicalBlockToFile(block);
            }
            saveLsmLevelsToDisk();

            System.out.println("KVSSD cleanup completed: memtable + SSTables persisted");
        } catch (Exception e) {
            System.err.println("Failed to cleanup KVSSD: " + e.getMessage());
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
        public boolean valid;               // 有效性
        public List<Pair<String, String>> data; // KV 数据
        public Pair<String, String> keyRange; // 键范围
        public int refCount;                // 引用计数

        public PhysicalPage(String ppa) {
            this.ppa = ppa;
            this.valid = true;
            this.data = new ArrayList<>();
            this.keyRange = new Pair<>("", "");
            this.refCount = 1;
        }

        /**
         * 更新键范围
         */
        public void updateKeyRange() {
            if (data.isEmpty()) {
                keyRange = new Pair<>("", "");
                return;
            }
            String minKey = data.get(0).first;
            String maxKey = data.get(0).first;
            for (Pair<String, String> kv : data) {
                if (kv.first.compareTo(minKey) < 0) minKey = kv.first;
                if (kv.first.compareTo(maxKey) > 0) maxKey = kv.first;
            }
            keyRange = new Pair<>(minKey, maxKey);
        }
    }

    /**
     * 物理块类（含持久化所需的元数据）
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

        /**
         * 计算无效页比例
         */
        public double getInvalidRatio() {
            if (pages.isEmpty()) return 0.0;
            long invalidCount = pages.stream()
                    .filter(page -> page != null && !page.valid)
                    .count();
            return (double) invalidCount / pages.size();
        }

        /**
         * 判断块是否已满
         */
        public boolean isFull() {
            return pages.stream().filter(Objects::nonNull).count() >= pages.size();
        }

        /**
         * 添加页到块
         */
        public boolean addPage(int pageNo, PhysicalPage page) {
            if (pageNo < 0 || pageNo >= pages.size() || pages.get(pageNo) != null) {
                return false;
            }
            pages.set(pageNo, page);
            return true;
        }
    }

    /**
     * SSTable 类（含持久化所需的元数据）
     */
    public static class SSTable {
        public long sstId;                 // SST ID
        public int level;                  // 所属层级
        public PhysicalPage metadataPage;  // 元数据页
        public List<PhysicalPage> kvPages; // KV 页
        public Pair<String, String> keyRange; // 键范围
        public int pageCounter;            // 页来源层级计数器
        public int kvpairSize;            //
        public String metadataPagePpa; // Meta Page指针（如"0_0"）

        public SSTable(long sstId, int level) {
            this.sstId = sstId;
            this.level = level;
            this.kvPages = new ArrayList<>();
            this.keyRange = new Pair<>("", "");
            this.pageCounter = 1;
        }

        /**
         * 更新键范围
         */
        public void updateKeyRange(List<PhysicalPage> kvPages) {
            if (kvPages.isEmpty()) {
                keyRange = new Pair<>("", "");
                return;
            }
            String minKey = kvPages.get(0).keyRange.first;
            String maxKey = kvPages.get(0).keyRange.second;
            for (PhysicalPage page : kvPages) {
                if (page.keyRange.first.compareTo(minKey) < 0) minKey = page.keyRange.first;
                if (page.keyRange.second.compareTo(maxKey) > 0) maxKey = page.keyRange.second;
            }
            keyRange = new Pair<>(minKey, maxKey);
        }
    }

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
    /**
     * 测试方法（验证持久化）
     */
    public static void main(String[] args) {
        KVSSD kvssd = new KVSSD();
        String value=kvssd.get("user793670813879");
        if(value!=null){
            System.out.println("Read success!");
        }else{
            System.out.println("Read fail!");
        }
    }
}
