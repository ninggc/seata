package io.seata.mongo.tx.support;

import io.seata.mongo.tx.mongo.bo.CustomUndoRecordBO.OpeType;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @description: 前后镜像生成模板
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/8 10:43
 * @version:v1.0
 */
public interface MongoUndoLogImageTemplate {

    /**
     * 单结果集执行
     * @param collectionName
     * @param opeType
     * @param condition
     * @param supplier
     * @return
     */
    Map singleExecuteWithCondition(String collectionName, OpeType opeType, Map<String, Object> condition, Supplier<Map> supplier);

    /**
     * 多结果集执行
     * @param collectionName
     * @param opeType
     * @param condition
     * @param supplier
     * @return
     */
    List<Map> multiExecuteWithCondition(String collectionName, OpeType opeType, Map<String, Object> condition, Supplier<List<Map>> supplier);

    /**
     * 对于保存生成前后镜像
     *
     * @param <T>
     * @param collectionName
     * @param insertSupplier
     * @param mongoDataId
     * @return
     */
    <T> T executeInsert(String collectionName, Supplier<T> insertSupplier, String mongoDataId);

    /**
     * 对于保存生成前后镜像
     *
     * @param collectionName
     * @param insertSupplier
     * @param <T>
     * @return
     */
    <T> Collection<T> executeBatchInsert(String collectionName, Supplier<Collection<T>> insertSupplier);

}
