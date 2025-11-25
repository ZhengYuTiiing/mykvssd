package com.ssd;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 键值对工具类
 */
public class Pair<K, V> {
    public K first;
    public V second;
    public long addr;
    public long keyNum;

    private static final Pattern USER_PATTERN = Pattern.compile("user(\\d+)");
    public Pair(K first, V second) {
        this.first = first;
        this.second = second;
        // 修复类型转换问题，只在 K 是 String 类型时才解析 keyNum
        if (first instanceof String) {
            this.keyNum = parseKeyNum((String) first);
        } else {
            this.keyNum = 0; // 或者其他默认值
        }
    }
    // 如果需要，可以添加 getter 和 setter 方法
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

        // 对于不符合user+数字格式的键，可以考虑其他处理方式：
        // 1. 返回默认值
        return 0;

        // 2. 或者使用键的哈希值作为keyNum
        // return Math.abs((long)key.hashCode());

        // 3. 或者抛出更详细的异常信息（不推荐，会影响系统稳定性）
        // throw new IllegalArgumentException("Invalid key format: " + key);
    }

    public long getAddr() {
        return addr;
    }

    public void setAddr(long addr) {
        this.addr = addr;
    }
    public long getKeyNum() {
        return keyNum;
    }
    public void setKeyNum(long keyNum) {
        this.keyNum = keyNum;
    }
}