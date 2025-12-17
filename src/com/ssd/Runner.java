
package com.ssd;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Runner {
    // 日期格式化：用于结果文件名唯一性
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd_HHmmss");
    // 结果文件统一存储目录
    private static final String RESULT_DIR = "kv_workload_results/";
    // 存储目录基础路径：所有workload的存储目录基于此路径拼接
    private static final String BASE_PERSIST_DIR = "./data2/";

    public static void main(String[] args) {
        // 1. 参数校验：确保传入至少1个CSV workload路径
        if (args.length < 1) {
            System.out.println("Usage: java KVWorkloadRunner <workload1.csv> [workload2.csv] ... [workloadN.csv]");
            System.out.println("Example: java KVWorkloadRunner csv/dedup.csv csv/var.csv csv/ycsb.csv");
            return;
        }

        // 2. 初始化结果目录（不存在则自动创建）
        File resultDir = new File(RESULT_DIR);
        if (!resultDir.exists()) {
            if (resultDir.mkdirs()) {
                System.out.println("Successfully created result directory: " + resultDir.getAbsolutePath());
            } else {
                System.err.println("Failed to create result directory: " + resultDir.getAbsolutePath());
                return;
            }
        }

        // 3. 批量处理每个CSV workload（循环遍历所有传入文件）
        for (String workloadFile : args) {
            System.out.println("\n==================================================");
            System.out.println("Starting workload: " + workloadFile);
            System.out.println("==================================================");

            // 运行单个workload并生成独立结果文件
            runSingleWorkload(workloadFile, resultDir);
        }

        for (String workloadFile : args) {
            System.out.println("\n==================================================");
            System.out.println("Starting workload: " + workloadFile);
            System.out.println("==================================================");

            // 运行单个workload并生成独立结果文件
            runSingleWorkload_MYKVSSD(workloadFile, resultDir);
        }

        // 4. 所有workload运行完成提示
        System.out.println("\n=== All workloads executed! Results saved to: " + resultDir.getAbsolutePath() + " ===");
    }

    /**
     * 运行单个CSV workload，动态设置Constants.PERSIST_DIR，生成独立结果文件
     * @param workloadFile CSV workload文件路径（相对/绝对路径均可）
     * @param resultDir 结果文件存储目录
     */
    private static void runSingleWorkload(String workloadFile, File resultDir) {
        // 步骤1：提取workload名称（如"csv/dedup.csv" → "dedup"，用于拼接存储目录和结果文件名）
        String workloadName = extractWorkloadName(workloadFile);
        // 步骤2：动态生成当前workload的存储目录（格式：BASE_PERSIST_DIR + 名称 + "_data/"）
        String dynamicPersistDir = BASE_PERSIST_DIR + workloadName + "_kvssd_data/";

        // 步骤3：修改Constants类的PERSIST_DIR静态变量（核心：切换存储目录）
        Constants.PERSIST_DIR = dynamicPersistDir;
        System.out.println("Updated Constants.PERSIST_DIR to: " + Constants.PERSIST_DIR);
        // 修改 PERSIST_DIR 后，需要重新计算所有依赖它的路径
        Constants.SST_DIR = Constants.PERSIST_DIR + "ssts/";
        Constants.BLOCK_META_DIR = Constants.PERSIST_DIR + "block_meta/";
        Constants.KEY_RANGE_TREE_FILE = Constants.PERSIST_DIR + "key_range_tree.dat";
        Constants.MEMTABLE_FILE = Constants.PERSIST_DIR + "memtable.data";
        Constants.LSM_LEVELS_FILE = Constants.PERSIST_DIR + "lsm_levels.data";
        Constants.MODEL_DIR = Constants.PERSIST_DIR + "models/";
        Constants.META_BLOCK_META_DIR = Constants.PERSIST_DIR + "meta_blocks/";
        Constants.BLOCK_DIR = Constants.PERSIST_DIR + "blocks/";

        // 步骤4：自动创建存储目录（避免路径不存在导致IO异常）
        File persistDirFile = new File(dynamicPersistDir);
        if (!persistDirFile.exists()) {
            if (persistDirFile.mkdirs()) {
                System.out.println("Created storage directory: " + dynamicPersistDir);
            } else {
                System.err.println("Failed to create storage directory: " + dynamicPersistDir);
                return; // 目录创建失败，终止当前workload
            }
        }

        // 步骤5：初始化KVSSD模拟器和统计变量
        KVSSD kvssd = new KVSSD(); // KVSSD会自动读取Constants.PERSIST_DIR
        long totalOps = 0;
        long readOps = 0, writeOps = 0;
        long startTime = System.nanoTime(); // 记录开始时间（用于计算吞吐量）
        BufferedWriter resultWriter = null; // 结果文件写入流

        try {
            // 步骤6：创建当前workload的结果文件（文件名：时间戳_名称_result.txt）
            String resultFileName = DATE_FORMAT.format(new Date()) + "_" + workloadName + "_kvssd_result.txt";
            File resultFile = new File(resultDir, resultFileName);
            resultWriter = new BufferedWriter(new FileWriter(resultFile));
            System.out.println("Result file will be saved to: " + resultFile.getAbsolutePath());

            // 步骤7：写入结果文件表头信息（记录关键配置，便于后续追溯）
            resultWriter.write("=== KV Workload Execution Result ===");
            resultWriter.newLine();
            resultWriter.write("Workload File: " + workloadFile);
            resultWriter.newLine();
            resultWriter.write("Storage Directory (PERSIST_DIR): " + Constants.PERSIST_DIR);
            resultWriter.newLine();
            resultWriter.write("Execution Start Time: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
            resultWriter.newLine();

            // 步骤8：读取CSV文件并处理操作（跳过表头，处理每一行KV操作）
            BufferedReader csvReader = new BufferedReader(new FileReader(workloadFile));
            String csvLine = csvReader.readLine(); // 读取并跳过CSV表头（op,key_str,value_str）

            // 处理空CSV文件场景
            if (csvLine == null) {
                String errorMsg = "Error: CSV file is empty - " + workloadFile;
                System.err.println(errorMsg);
                resultWriter.write(errorMsg);
                csvReader.close();
                return;
            }
            // 记录跳过的表头到结果文件
            resultWriter.write("Skipped CSV Header: " + csvLine);
            resultWriter.newLine();
            resultWriter.newLine();

            // 循环处理CSV每一行操作
            while ((csvLine = csvReader.readLine()) != null) {
                csvLine = csvLine.trim();
                if (csvLine.isEmpty()) continue; // 跳过空行

                // 分割CSV行（最多分割3段：op, key_str, value_str，避免Value含逗号导致分割错误）
                String[] csvParts = csvLine.split(",", 3);
                if (csvParts.length < 2) { // 至少需要"操作类型"和"Key"
                    String skipMsg = "Skipped invalid line (format error): " + csvLine;
                    System.err.println(skipMsg);
                    resultWriter.write(skipMsg);
                    resultWriter.newLine();
                    continue;
                }

                // 提取操作类型、Key、Value（均为明文字符串，无需解码）
                String opType = csvParts[0].trim();
                String key = csvParts[1].trim();
                String value = csvParts.length >= 3 ? csvParts[2].trim() : "";

                // 执行对应的读/写操作
                if ("W".equalsIgnoreCase(opType)) { // 写操作（忽略大小写，兼容"W"/"w"）
                    kvssd.put(key, value);
                    writeOps++;
                } else if ("R".equalsIgnoreCase(opType)) { // 读操作（忽略大小写）
                    kvssd.get(key);
                    readOps++;
                } else { // 未知操作类型，跳过
                    String skipMsg = "Skipped unknown operation type (" + opType + "): " + csvLine;
                    System.err.println(skipMsg);
                    resultWriter.write(skipMsg);
                    resultWriter.newLine();
                    continue;
                }

                // 统计总操作数，并每10000次操作打印进度
                totalOps++;
                if (totalOps % 10000 == 0) {
                    String progressMsg = "Processed " + totalOps + " operations...";
                    System.out.println(progressMsg);
                    resultWriter.write(progressMsg);
                    resultWriter.newLine();
                }
            }
            csvReader.close(); // 关闭CSV读取流

            // 步骤9：计算性能指标（总耗时、吞吐量）
            long endTime = System.nanoTime();
            double elapsedSeconds = (endTime - startTime) / 1e9; // 纳秒转秒
            double throughput = totalOps > 0 ? totalOps / elapsedSeconds : 0.0; // 每秒操作数

            // 步骤10：清理KVSSD资源并获取内部统计数据
            kvssd.cleanup();
            KVSSD.Stats kvssdStats = kvssd.getStats();
            // 空指针防护：若获取不到统计数据，初始化空实例
            if (kvssdStats == null) {
                kvssdStats = new KVSSD.Stats();
                String warnMsg = "Warning: Failed to get stats from KVSSD6, using empty stats.";
                System.err.println(warnMsg);
                resultWriter.write(warnMsg);
                resultWriter.newLine();
            }

            // 步骤11：写入完整统计结果到文件（基础指标 + KVSSD详细统计）
            resultWriter.write("==================================================");
            resultWriter.newLine();
            // 基础操作统计（全部用write+String.format）
            resultWriter.write("                 Basic Operation Stats            ");
            resultWriter.write(System.lineSeparator());
            resultWriter.write("==================================================");
            resultWriter.write(System.lineSeparator());
// 总操作数（格式化千位分隔）
            resultWriter.write(String.format("Total Operations: %,d (Read: %d, Write: %d)%n", totalOps, readOps, writeOps));
// 耗时（保留2位小数）
            resultWriter.write(String.format("Elapsed Time: %.2f seconds%n", elapsedSeconds));
// 吞吐量（保留2位小数）
            resultWriter.write(String.format("Throughput: %.2f operations/second%n", throughput));
            resultWriter.write(System.lineSeparator());

// Flash访问分布（最后一行的8+ times）
            resultWriter.write("--------------------------------------------------");
            resultWriter.write(System.lineSeparator());
            resultWriter.write("Read Flash Access Distribution (Times per Read):");
            resultWriter.write(System.lineSeparator());
            resultWriter.write(String.format("  0 times: %,d%n", kvssdStats.read0Flash));
            resultWriter.write(String.format("  1 times: %,d%n", kvssdStats.read1Flash));
            resultWriter.write(String.format("  2 times: %,d%n", kvssdStats.read2Flash));
            resultWriter.write(String.format("  3 times: %,d%n", kvssdStats.read3Flash));
            resultWriter.write(String.format("  4 times: %,d%n", kvssdStats.read4Flash));
            resultWriter.write(String.format("  5 times: %,d%n", kvssdStats.read5Flash));
            resultWriter.write(String.format("  6 times: %,d%n", kvssdStats.read6Flash));
            resultWriter.write(String.format("  7 times: %,d%n", kvssdStats.read7Flash));
            resultWriter.write(String.format("  8 times: %,d%n", kvssdStats.read8Flash));
            resultWriter.write(String.format("  8+ times: %,d%n", kvssdStats.readMoreFlash));
            resultWriter.newLine();

            // 步骤12：控制台打印当前workload完成信息
            System.out.println("==================================================");
            System.out.printf("Workload '%s' completed successfully!%n", workloadName);
            System.out.printf("Total Ops: %,d | Throughput: %.2f ops/s%n", totalOps, throughput);
            System.out.println("Result saved to: " + resultFile.getAbsolutePath());

        } catch (IOException e) {
            // 异常处理：打印错误信息并写入结果文件
            String errorMsg = "Error processing workload '" + workloadFile + "': " + e.getMessage();
            System.err.println(errorMsg);
            e.printStackTrace();
            if (resultWriter != null) {
                try {
                    resultWriter.write(errorMsg);
                    resultWriter.newLine();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        } finally {
            // 步骤13：确保结果文件流关闭（避免资源泄漏）
            if (resultWriter != null) {
                try {
                    resultWriter.close();
                } catch (IOException e) {
                    System.err.println("Error closing result writer: " + e.getMessage());
                }
            }
            // 可选：清理空存储目录（若当前workload无数据写入）
            if (persistDirFile.exists() && persistDirFile.listFiles().length == 0) {
                if (persistDirFile.delete()) {
                    System.out.println("Deleted empty storage directory: " + dynamicPersistDir);
                }
            }
        }
    }

    private static void runSingleWorkload_MYKVSSD(String workloadFile, File resultDir) {
        // 步骤1：提取workload名称（如"csv/dedup.csv" → "dedup"，用于拼接存储目录和结果文件名）
        String workloadName = extractWorkloadName(workloadFile);
        // 步骤2：动态生成当前workload的存储目录（格式：BASE_PERSIST_DIR + 名称 + "_data/"）
        String dynamicPersistDir = BASE_PERSIST_DIR + workloadName + "_mykvssd_data/";

        // 步骤3：修改Constants类的PERSIST_DIR静态变量（核心：切换存储目录）
        Constants.PERSIST_DIR = dynamicPersistDir;
        System.out.println("Updated Constants.PERSIST_DIR to: " + Constants.PERSIST_DIR);
        Constants.SST_DIR = Constants.PERSIST_DIR + "ssts/";
        Constants.BLOCK_META_DIR = Constants.PERSIST_DIR + "block_meta/";
        Constants.KEY_RANGE_TREE_FILE = Constants.PERSIST_DIR + "key_range_tree.dat";
        Constants.MEMTABLE_FILE = Constants.PERSIST_DIR + "memtable.data";
        Constants.LSM_LEVELS_FILE = Constants.PERSIST_DIR + "lsm_levels.data";
        Constants.MODEL_DIR = Constants.PERSIST_DIR + "models/";
        Constants.META_BLOCK_META_DIR = Constants.PERSIST_DIR + "meta_blocks/";
        Constants.BLOCK_DIR = Constants.PERSIST_DIR + "blocks/";

        // 步骤4：自动创建存储目录（避免路径不存在导致IO异常）
        File persistDirFile = new File(dynamicPersistDir);
        if (!persistDirFile.exists()) {
            if (persistDirFile.mkdirs()) {
                System.out.println("Created storage directory: " + dynamicPersistDir);
            } else {
                System.err.println("Failed to create storage directory: " + dynamicPersistDir);
                return; // 目录创建失败，终止当前workload
            }
        }

        // 步骤5：初始化KVSSD模拟器和统计变量
        MYKVSSD kvssd = new MYKVSSD(); // KVSSD会自动读取Constants.PERSIST_DIR
        long totalOps = 0;
        long readOps = 0, writeOps = 0;
        long startTime = System.nanoTime(); // 记录开始时间（用于计算吞吐量）
        BufferedWriter resultWriter = null; // 结果文件写入流

        try {
            // 步骤6：创建当前workload的结果文件（文件名：时间戳_名称_result.txt）
            String resultFileName = DATE_FORMAT.format(new Date()) + "_" + workloadName + "_mykvssd_result.txt";
            File resultFile = new File(resultDir, resultFileName);
            resultWriter = new BufferedWriter(new FileWriter(resultFile));
            System.out.println("Result file will be saved to: " + resultFile.getAbsolutePath());

            // 步骤7：写入结果文件表头信息（记录关键配置，便于后续追溯）
            resultWriter.write("=== KV Workload Execution Result ===");
            resultWriter.newLine();
            resultWriter.write("Workload File: " + workloadFile);
            resultWriter.newLine();
            resultWriter.write("Storage Directory (PERSIST_DIR): " + Constants.PERSIST_DIR);
            resultWriter.newLine();
            resultWriter.write("Execution Start Time: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
            resultWriter.newLine();

            // 步骤8：读取CSV文件并处理操作（跳过表头，处理每一行KV操作）
            BufferedReader csvReader = new BufferedReader(new FileReader(workloadFile));
            String csvLine = csvReader.readLine(); // 读取并跳过CSV表头（op,key_str,value_str）

            // 处理空CSV文件场景
            if (csvLine == null) {
                String errorMsg = "Error: CSV file is empty - " + workloadFile;
                System.err.println(errorMsg);
                resultWriter.write(errorMsg);
                csvReader.close();
                return;
            }
            // 记录跳过的表头到结果文件
            resultWriter.write("Skipped CSV Header: " + csvLine);
            resultWriter.newLine();
            resultWriter.newLine();

            // 循环处理CSV每一行操作
            while ((csvLine = csvReader.readLine()) != null) {
                csvLine = csvLine.trim();
                if (csvLine.isEmpty()) continue; // 跳过空行

                // 分割CSV行（最多分割3段：op, key_str, value_str，避免Value含逗号导致分割错误）
                String[] csvParts = csvLine.split(",", 3);
                if (csvParts.length < 2) { // 至少需要"操作类型"和"Key"
                    String skipMsg = "Skipped invalid line (format error): " + csvLine;
                    System.err.println(skipMsg);
                    resultWriter.write(skipMsg);
                    resultWriter.newLine();
                    continue;
                }

                // 提取操作类型、Key、Value（均为明文字符串，无需解码）
                String opType = csvParts[0].trim();
                String key = csvParts[1].trim();
                String value = csvParts.length >= 3 ? csvParts[2].trim() : "";

                // 执行对应的读/写操作
                if ("W".equalsIgnoreCase(opType)) { // 写操作（忽略大小写，兼容"W"/"w"）
                    kvssd.put(key, value);
                    writeOps++;
                } else if ("R".equalsIgnoreCase(opType)) { // 读操作（忽略大小写）
                    kvssd.get(key);
                    readOps++;
                } else { // 未知操作类型，跳过
                    String skipMsg = "Skipped unknown operation type (" + opType + "): " + csvLine;
                    System.err.println(skipMsg);
                    resultWriter.write(skipMsg);
                    resultWriter.newLine();
                    continue;
                }

                // 统计总操作数，并每10000次操作打印进度
                totalOps++;
                if (totalOps % 10000 == 0) {
                    String progressMsg = "Processed " + totalOps + " operations...";
                    System.out.println(progressMsg);
                    resultWriter.write(progressMsg);
                    resultWriter.newLine();
                }
            }
            csvReader.close(); // 关闭CSV读取流

            // 步骤9：计算性能指标（总耗时、吞吐量）
            long endTime = System.nanoTime();
            double elapsedSeconds = (endTime - startTime) / 1e9; // 纳秒转秒
            double throughput = totalOps > 0 ? totalOps / elapsedSeconds : 0.0; // 每秒操作数

            // 步骤10：清理KVSSD资源并获取内部统计数据
            kvssd.shutdown();
            MYKVSSD.Stats kvssdStats = kvssd.getStats();
            // 空指针防护：若获取不到统计数据，初始化空实例
            if (kvssdStats == null) {
                kvssdStats = new MYKVSSD.Stats();
                String warnMsg = "Warning: Failed to get stats from MYKVSSD, using empty stats.";
                System.err.println(warnMsg);
                resultWriter.write(warnMsg);
                resultWriter.newLine();
            }

            // 步骤11：写入完整统计结果到文件（基础指标 + KVSSD详细统计）
            resultWriter.write("==================================================");
            resultWriter.newLine();
            // 基础操作统计（全部用write+String.format）
            resultWriter.write("                 Basic Operation Stats            ");
            resultWriter.write(System.lineSeparator());
            resultWriter.write("==================================================");
            resultWriter.write(System.lineSeparator());
// 总操作数（格式化千位分隔）
            resultWriter.write(String.format("Total Operations: %,d (Read: %d, Write: %d)%n", totalOps, readOps, writeOps));
// 耗时（保留2位小数）
            resultWriter.write(String.format("Elapsed Time: %.2f seconds%n", elapsedSeconds));
// 吞吐量（保留2位小数）
            resultWriter.write(String.format("Throughput: %.2f operations/second%n", throughput));
            resultWriter.write(System.lineSeparator());

// Flash访问分布（最后一行的8+ times）
            resultWriter.write("--------------------------------------------------");
            resultWriter.write(System.lineSeparator());
            resultWriter.write("Read Flash Access Distribution (Times per Read):");
            resultWriter.write(System.lineSeparator());
            resultWriter.write(String.format("  0 times: %,d%n", kvssdStats.read0Flash));
            resultWriter.write(String.format("  1 times: %,d%n", kvssdStats.read1Flash));
            resultWriter.write(String.format("  2 times: %,d%n", kvssdStats.read2Flash));
            resultWriter.write(String.format("  3 times: %,d%n", kvssdStats.read3Flash));
            resultWriter.write(String.format("  4 times: %,d%n", kvssdStats.read4Flash));
            resultWriter.write(String.format("  5 times: %,d%n", kvssdStats.read5Flash));
            resultWriter.write(String.format("  6 times: %,d%n", kvssdStats.read6Flash));
            resultWriter.write(String.format("  7 times: %,d%n", kvssdStats.read7Flash));
            resultWriter.write(String.format("  8 times: %,d%n", kvssdStats.read8Flash));
            resultWriter.write(String.format("  8+ times: %,d%n", kvssdStats.readMoreFlash));
            resultWriter.newLine();
            // 添加预测准确率统计信息
            resultWriter.write("--------------------------------------------------");
            resultWriter.write("Prediction Accuracy Statistics:");
            resultWriter.write(String.format("Prediction Accuracy: %.2f%%%n", kvssdStats.getPredictionAccuracy()));
            resultWriter.write(String.format("Predict Success: %,d%n", kvssdStats.predictSuccess));
            resultWriter.write(String.format("Predict Failures: %,d%n", kvssdStats.predictFailures));
            resultWriter.newLine();
            // 步骤12：控制台打印当前workload完成信息
            System.out.println("==================================================");
            System.out.printf("Workload '%s' completed successfully!%n", workloadName);
            System.out.printf("Total Ops: %,d | Throughput: %.2f ops/s%n", totalOps, throughput);
            System.out.println("Result saved to: " + resultFile.getAbsolutePath());

        } catch (IOException e) {
            // 异常处理：打印错误信息并写入结果文件
            String errorMsg = "Error processing workload '" + workloadFile + "': " + e.getMessage();
            System.err.println(errorMsg);
            e.printStackTrace();
            if (resultWriter != null) {
                try {
                    resultWriter.write(errorMsg);
                    resultWriter.newLine();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        } finally {
            // 步骤13：确保结果文件流关闭（避免资源泄漏）
            if (resultWriter != null) {
                try {
                    resultWriter.close();
                } catch (IOException e) {
                    System.err.println("Error closing result writer: " + e.getMessage());
                }
            }
            // 可选：清理空存储目录（若当前workload无数据写入）
            if (persistDirFile.exists() && persistDirFile.listFiles().length == 0) {
                if (persistDirFile.delete()) {
                    System.out.println("Deleted empty storage directory: " + dynamicPersistDir);
                }
            }
        }
    }


    /**
     * 辅助方法：从CSV文件路径中提取workload名称（用于拼接目录和文件名）
     * 示例："csv/dedup.csv" → "dedup"，"var.csv" → "var"，"/home/data/ycsb.csv" → "ycsb"
     * @param filePath CSV文件路径
     * @return 提取的workload名称
     */
    private static String extractWorkloadName(String filePath) {
        File file = new File(filePath);
        String fileName = file.getName(); // 获取文件名（如"dedup.csv"）
        int dotIndex = fileName.lastIndexOf('.'); // 找到后缀名分隔符"."的位置
        // 截取"."之前的部分作为名称（无后缀则返回完整文件名）
        return dotIndex > 0 ? fileName.substring(0, dotIndex) : fileName;
    }
}