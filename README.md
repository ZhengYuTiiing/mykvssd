# mykvssd
mykvssd for my paper,using learned index
#how to use
1. clone this project
2. compile it
  ```
  javac -d classes src/com/ssd/*.java
  ```
3. run it
  ```
  java -cp .\classes\ com.ssd.MYKVSSD
  ```
4. test it,using YCSB
5. clone YCSB project
  ```
  git clone https://github.com/brianfrankcooper/YCSB.git
  ```
6. compile YCSB project
  ```
  cd YCSB
  mvn clean package
  ```
7.create learned-kvssd directory,/YCSB/mykvssd/src/main/java/site/ycsb/db/mykvssd
  ```
  /YCSB/mykvssd/src/main/java/site/ycsb/db/mykvssd
  ```
8. copy mykvssd.java ,Constants.java , Pair.java
  ```
  cp src/com/ssd/MYKVSSD.java /YCSB/mykvssd/src/main/java/site/ycsb/db/mykvssd 
  cp src/com/ssd/Constants.java /YCSB/mykvssd/src/main/java/site/ycsb/db/mykvssd
  cp src/com/ssd/Pair.java /YCSB/mykvssd/src/main/java/site/ycsb/db/mykvssd 
  ```
9.create MYKVSSDClient.java
```angular2html
package site.ycsb.db.mykvssd;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import site.ycsb.DB;

import site.ycsb.ByteIterator;
import site.ycsb.StringByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

public class MYKVSSDClient extends DB {

    private MYKVSSD kvssd;

    @Override
    public void init() throws DBException{
       kvssd = new MYKVSSD();  // 创建KVSSD实例
    }

    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        String value = kvssd.get(key);
        if (value != null) {
            result.put(key, new StringByteIterator(value));
            return Status.OK;
        }
        System.err.println("readkey : "+key+" not found.");
        return Status.NOT_FOUND;
    }
  // 2. 插入方法（适配KVSSD单value）
    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        // 第一步：判空values（避免YCSB传入空映射）
        if (values == null || values.isEmpty()) {
            System.err.println("Error: YCSB passed empty values for key: " + key);
            return Status.ERROR;
        }

        // 第二步：提取第一个字段的value（不依赖字段名，兼容任意单字段配置）
        ByteIterator byteIter = values.values().iterator().next();
        if (byteIter == null) {
            System.err.println("Error: Empty value for key: " + key);
            return Status.ERROR;
        }

        // 第三步：转为字符串并写入KVSSD（KVSSD只需要key和单value）
        String value = byteIter.toString();
        kvssd.put(key, value); // 直接调用KVSSD的put（key-value映射）
        return Status.OK;
    }
    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> results) {
        // 模拟扫描从 startkey 开始的记录
        // 实际上，你应该根据KVSSD的API来实现具体的扫描逻辑
        
        int scanned = 0;
        while (scanned < recordcount) {
            String key = startkey + "_" + scanned;  // 假设扫描的键名是通过startkey拼接
            String value = kvssd.get(key);  // 从KVSSD中获取值

            if (value != null) {
                // 构造一个结果，假设只有一个字段 "field0"
                HashMap<String, ByteIterator> record = new HashMap<>();
                record.put("field0", new StringByteIterator(value));
                results.add(record);
                scanned++;
            } else {
                // 如果找不到对应的值，可以选择返回 NOT_FOUND 或跳过
                break;
            }
        }

        return Status.OK;
    }
    @Override
    public Status delete(String table, String key) {
        kvssd.put(key, null);  // 删除相当于插入一个空值
        return Status.OK;
    }

    @Override
    public void cleanup() throws DBException {
           kvssd.shutdown();  // 删除相当于插入一个空值
    }
    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values){
          return insert(table, key, values); 
    }

}

```
10. create pom.xml under YCSB/mykvssd
```angular2html
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <parent>
    <groupId>site.ycsb</groupId>
    <artifactId>binding-parent</artifactId>
    <version>0.18.0-SNAPSHOT</version>
    <relativePath>../binding-parent/</relativePath>
  </parent>

  <artifactId>mykvssd-binding</artifactId>
  <name>MYKVSSD Binding</name>
  <packaging>jar</packaging>

  <dependencies>
    <dependency>
      <groupId>site.ycsb</groupId>
      <artifactId>core</artifactId>
      <version>${project.version}</version>
      <scope>provided</scope>
    </dependency>
  </dependencies>
</project>
```
11. modify pom.xml ubder YCSB ,add mykvssd to YCSB/pom.xml
```angular2html
    <module>mykvssd</module>
```
12. modify YCSB/bin/ycsb
```
"mykvssd"      : "site.ycsb.db.mykvssd.MYKVSSDClient"
```  
```angular2html
def get_classpath_from_maven(module):
    output_file = tempfile.NamedTemporaryFile(delete=True)
    cmd = ["mvn", "-B", "-pl", "site.ycsb:" + module,
           "-am", "package", "-DskipTests", "-Dcheckstyle.skip=true",  # 添加 -Dcheckstyle.skip=true
           "dependency:list",
           "-DoutputAbsoluteArtifactFilename",
           "-DappendOutput=false",
           "-DoutputFile=" + output_file.name
           ]
    debug("Running '" + " ".join(cmd) + "'")
    subprocess.check_call(cmd)
```