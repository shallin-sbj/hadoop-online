package com.scl.hadoop.hbase.service;

import java.util.Map;
import java.util.TreeMap;

/**
 * hbase 基本操作场景
 */
public interface BaseHbaseOptService {

    /**
     * 查询分页查询所有数据所有数据
     *
     * @param tableName
     * @param prefix
     * @param pageSize
     * @return 有序的TreeMap
     */
    TreeMap<String, Map<String, String>> queryAllByPage(String tableName, String prefix, int pageSize);

    /**
     * 分页查询数据
     *
     * @param tableName
     * @param prefix
     * @param lastRowKey
     * @param pageSize
     * @return 有序的TreeMap
     */
    TreeMap<String, Map<String, String>> queryDataByPageSize(String tableName, String prefix, String lastRowKey, int pageSize);


}
