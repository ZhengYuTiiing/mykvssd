package com.ssd;
import java.util.regex.Pattern;
/**
 * 常量定义（含持久化路径）
 */
public class Constants {
    // Define flash operation timing constants
    public static final int FLASH_PAGE_READ_TIME_NS = 120000;    // 120 microseconds
    public static final int FLASH_PAGE_WRITE_TIME_NS = 320000;   // 320 microseconds
    public static final int FLASH_BLOCK_ERASE_TIME_NS = 3000000; // 3 milliseconds

    // 原有的模拟器常量
    public static final int PAGE_SIZE = 32 * 1024;       // 32KB 页大小
    public static final long BLOCK_SIZE = 4 * 1024 * 1024; // 4MB 块大小

    //public static final long BLOCK_SIZE = 8 * 32 * 1024; //
    public static final int MAX_MEMTABLE_SIZE =  4* 1024* 1024 - 32* 1024; // 4MB Memtable 阈值
    public static final int LEVEL_RATIO = 10;            // LSM 层级比例
    public static final double GC_THRESHOLD = 0.7;       // GC 触发阈值
    public static final int REMAP_THRESHOLD = 2;         // 重映射阈值

    // L0 压缩触发与批量大小（基于文件数的触发条件）
    public static final int L0_COMPACTION_TRIGGER = 1000;   // 当 L0 中的 SSTable 数量达到该阈值时触发压缩
    public static final int L0_COMPACTION_BATCH = 4;     // 每次从 L0 选取参与压缩的 SSTable 个数

    // 按层的 SSTable 数量上限（仅用于“按个数触发”策略）。
    // 下标=层级，值=该层允许的最大 SSTable 个数。超出即触发向下一层压缩。
    public static final int[] LEVEL_SST_COUNT_LIMITS = new int[]{
            1,  // L0 上限
            10,  // L1 上限
            100,  // L2 上限
            1000, // L3 上限
            10000, // L4 上限
            100000 // L5 及以上默认上限
    };

    // 新增：持久化路径（确保程序有权限读写）
    public static String PERSIST_DIR = "./data2/kvssd_mykvssd_data/"; // 总持久化目录
    public static String SST_DIR = PERSIST_DIR + "ssts/";   // SSTable 存储目录
    public static  String BLOCK_META_DIR = PERSIST_DIR + "block_meta/"; // 物理块元数据目录
    public static final String SST_META_SUFFIX = ".txt";         // SSTable 元数据文件后缀
    public static final String BLOCK_META_SUFFIX = ".txt";     // 物理块元数据文件后缀
    public static  String KEY_RANGE_TREE_FILE = PERSIST_DIR + "key_range_tree.dat"; // 键范围树文件
    public static  String MEMTABLE_FILE = PERSIST_DIR + "memtable.data";
    // LSM层级元数据持久化路径
    public static  String LSM_LEVELS_FILE = PERSIST_DIR + "lsm_levels.data";
    public static  String MODEL_DIR = PERSIST_DIR + "models/";
    // 元数据区配置（独立于数据区）
    public static final long META_ZONE_BLOCK_COUNT = 1000; // 元数据区占用10个块
    public static  String META_BLOCK_META_DIR = PERSIST_DIR + "meta_blocks/"; // 元数据区块元数据目录
    public static  String BLOCK_DIR = PERSIST_DIR + "blocks/";

    public static final int MAX_ERROR = 1 * PAGE_SIZE; // 最大允许误差：1个页（96KB）
    public static final Pattern USER_PATTERN = Pattern.compile("user(\\d+)");
    public static final String KEY_ADDR_FILE = "key_addr.txt";
    public static final String PAGES_DIR = "pages";
    public static final String ERROR_DETAIL_FILE = "error_details.txt";
    // 布隆过滤器参数（动态计算，这里仅设置目标误判率）
    public static final double BLOOM_FALSE_POSITIVE_RATE = 0.05; // 目标误判率：1%（可调整）



}