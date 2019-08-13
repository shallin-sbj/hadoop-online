package com.scl.online.hbase.service.impl;

import com.scl.online.hbase.repository.HBaseService;
import com.scl.online.hbase.service.BaseHbaseOptService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.TreeMap;

@Slf4j
@Service
public class BaseHbaseOptServiceImpl implements BaseHbaseOptService {

    @Value("${HBase.pageSize:1000}")
    private int pageSize;

    @Value("${HBase.scanCaching:1000}")
    private int scanCaching;

    @Autowired
    private HBaseService hBaseService;

    @Override
    public TreeMap<String, Map<String, String>> queryAllByPage(String tableName, String prefix, int pageSize) {
        String lastRowKey = prefix;
        TreeMap<String, Map<String, String>> resultMap = new TreeMap<>();
        TreeMap<String, Map<String, String>> scannerPrefix = this.queryDataByPageSize(tableName, prefix, lastRowKey, pageSize);
        while (!scannerPrefix.isEmpty()) {
            resultMap.putAll(scannerPrefix);
            int size = scannerPrefix.size();
            if (size >= pageSize) {
                lastRowKey = scannerPrefix.lastKey();
                scannerPrefix = this.queryDataByPageSize(tableName, prefix, lastRowKey, pageSize);
            } else {
                break;
            }
        }
        return resultMap;

    }

    @Override
    public TreeMap<String, Map<String, String>> queryDataByPageSize(String tableName, String prefix, String lastRowKey, int pageSize) {
        return hBaseService.getResultScannerPrefixFilter(tableName, prefix, lastRowKey, pageSize);
    }
}
