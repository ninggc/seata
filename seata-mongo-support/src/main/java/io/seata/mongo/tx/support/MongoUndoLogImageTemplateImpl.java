package io.seata.mongo.tx.support;

import io.seata.core.context.RootContext;
import io.seata.mongo.tx.mongo.bo.CustomUndoRecordBO;
import io.seata.mongo.tx.mongo.bo.CustomUndoRecordBO.OpeType;
import io.seata.mongo.tx.mongo.util.MongoQueryUtil;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

/**
 * @description: 前后镜像生成模板
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/8 10:43
 * @version:v1.0
 */
public class MongoUndoLogImageTemplateImpl implements MongoUndoLogImageTemplate {

    private final MongoTemplate mongoTemplate;
    private final MongoSeataRmHelper mongoSeataRMHelper;

    public MongoUndoLogImageTemplateImpl(MongoTemplate mongoTemplate, MongoSeataRmHelper mongoSeataRMHelper) {
        this.mongoTemplate = mongoTemplate;
        this.mongoSeataRMHelper = mongoSeataRMHelper;
    }

    /**
     * 单结果集执行
     * @param collectionName
     * @param opeType
     * @param condition
     * @param supplier
     * @return
     */
    @Override
    public Map singleExecuteWithCondition(String collectionName, OpeType opeType, Map<String, Object> condition, Supplier<Map> supplier) {
        Map map = supplier.get();

        assembleUndoLog(Collections.singletonList(map), collectionName, opeType, condition);
        return map;
    }

    /**
     * 多结果集执行
     * @param collectionName
     * @param opeType
     * @param condition
     * @param supplier
     * @return
     */
    @Override
    public List<Map> multiExecuteWithCondition(String collectionName, OpeType opeType, Map<String, Object> condition, Supplier<List<Map>> supplier) {
        List<Map> maps = supplier.get();

        assembleUndoLog(maps, collectionName, opeType, condition);
        return maps;
    }

    private void assembleUndoLog(List<Map> beforeImage, String collectionName, OpeType opeType, Map<String, Object> condition) {
        String xid = RootContext.getXID();
        if (xid == null) {
            return;
        }

        List<Map> afterImage = mongoTemplate.find(MongoQueryUtil.queryGenerator(condition), Map.class, collectionName);

        // 添加undoLog
        Map<String, CustomUndoRecordBO> idAndBOMap = new HashMap<>();
        LocalDateTime opeTime = LocalDateTime.now();
        for (Map map : beforeImage) {
            String mongoDataId = MongoQueryUtil.getMongoDataId(map);
            CustomUndoRecordBO customUndoRecordBO = CustomUndoRecordBO.builder()
                .collectionName(collectionName)
                .id(mongoDataId)
                .opeTime(opeTime)
                .opeType(opeType)
                .beforeImage(map)
                .afterImage(null)
                .build();
            idAndBOMap.put(mongoDataId, customUndoRecordBO);
        }
        for (Map map : afterImage) {
            String mongoDataId = MongoQueryUtil.getMongoDataId(map);
            if (!idAndBOMap.containsKey(mongoDataId)) {
                CustomUndoRecordBO customUndoRecordBO = CustomUndoRecordBO.builder()
                    .collectionName(collectionName)
                    .id(mongoDataId)
                    .opeTime(opeTime)
                    .opeType(opeType)
                    .beforeImage(null)
                    .afterImage(null)
                    .build();
                idAndBOMap.put(mongoDataId, customUndoRecordBO);
            }

            idAndBOMap.get(mongoDataId).setAfterImage(map);
        }

        mongoSeataRMHelper.tempSaveUndoLog(xid, idAndBOMap.values(), collectionName);
    }

    /**
     * 对于保存生成前后镜像
     *
     * @param <T>
     * @param collectionName
     * @param insertSupplier
     * @param mongoDataId
     * @return
     */
    @Override
    public <T> T executeInsert(String collectionName, Supplier<T> insertSupplier, String mongoDataId) {
        Map beforeImage = null;
        if (mongoDataId != null) {
            // 应对insert，但是实际是update的情况
            beforeImage = mongoTemplate.findById(mongoDataId, Map.class, collectionName);
        }
        T result = insertSupplier.get();
        String xid = RootContext.getXID();
        if (xid == null) {
            // 无全局事务，不生成镜像
            return result;
        }

        // 添加undoLog
        mongoDataId = MongoQueryUtil.getMongoDataId(result);
        CustomUndoRecordBO customUndoRecordBO = CustomUndoRecordBO.builder()
            .collectionName(collectionName)
            .id(mongoDataId)
            .opeTime(LocalDateTime.now())
            .opeType(OpeType.INSERT)
            .beforeImage(beforeImage)
            .afterImage(mongoTemplate.findById(mongoDataId, Map.class, collectionName))
            .build();

        mongoSeataRMHelper.tempSaveUndoLog(xid, Collections.singleton(customUndoRecordBO), collectionName);
        return result;
    }

    /**
     * 对于保存生成前后镜像
     *
     * @param collectionName
     * @param insertSupplier
     * @param <T>
     * @return
     */
    @Override
    public <T> Collection<T> executeBatchInsert(String collectionName, Supplier<Collection<T>> insertSupplier) {
        Collection<T> result = insertSupplier.get();
        String xid = RootContext.getXID();
        if (xid == null) {
            // 无全局事务，不生成镜像
            return result;
        }

        Set<String> ids = result.stream().map(MongoQueryUtil::getMongoDataId).collect(Collectors.toSet());
        List<Map> afterImages = mongoTemplate.find(
            new Query().addCriteria(Criteria.where("id").in(ids))
            , Map.class
            , collectionName
        );

        // 添加undoLog
        List<CustomUndoRecordBO> customUndoRecordBOS = new ArrayList<>();
        LocalDateTime opeTime = LocalDateTime.now();
        for (Map map : afterImages) {
            String mongoDataId = MongoQueryUtil.getMongoDataId(result);
            customUndoRecordBOS.add(
                CustomUndoRecordBO.builder()
                    .collectionName(collectionName)
                    .id(mongoDataId)
                    .opeTime(opeTime)
                    .opeType(OpeType.INSERT)
                    .beforeImage(null)
                    .afterImage(map)
                    .build()
            );
        }

        mongoSeataRMHelper.tempSaveUndoLog(xid, customUndoRecordBOS, collectionName);
        return result;
    }

}
