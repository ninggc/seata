package io.seata.mongo.tx.mongo.util;

import io.seata.mongo.tx.mongo.bo.CustomUndoRecordBO;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.CollectionUtils;

/**
 * @description: 消费undolog
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/11 14:15
 * @version:v1.0
 */
@Slf4j
public class UndoLogConsumer implements BiConsumer<String, List<CustomUndoRecordBO>> {

    private final String xid;
    private final long branchId;
    private final MongoTemplate mongoTemplate;
    private final DirtyWriteStrategy dirtyWriteStrategy;

    public UndoLogConsumer(MongoTemplate mongoTemplate, DirtyWriteStrategy dirtyWriteStrategy, String xid, long branchId) {
        this.dirtyWriteStrategy = dirtyWriteStrategy;
        this.xid = xid;
        this.branchId = branchId;
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void accept(String collectionName, List<CustomUndoRecordBO> customUndoRecordBOS) {

        // 所有bo按照id分组
        Map<String, List<CustomUndoRecordBO>> idAndBOMap = customUndoRecordBOS.stream()
            .collect(Collectors.groupingBy(CustomUndoRecordBO::getId));
        // 所有po按照id分组
        Map<String, Map> idAndPOMap = mongoTemplate.find(buildInQuery(idAndBOMap.keySet()), Map.class, collectionName)
            .stream().collect(Collectors.toMap(MongoQueryUtil::getMongoDataId, po -> po));

        List<CustomUndoRecordBO> saveList = new ArrayList<>();
        List<CustomUndoRecordBO> updateList = new ArrayList<>();
        List<CustomUndoRecordBO> deleteList = new ArrayList<>();

        for (Entry<String, List<CustomUndoRecordBO>> entry : idAndBOMap.entrySet()) {
            List<CustomUndoRecordBO> bos = new ArrayList<>(entry.getValue());
            // 按照操作时间升序
            bos.sort(Comparator.comparing(CustomUndoRecordBO::getOpeTime));

            CustomUndoRecordBO firstBO = bos.get(0);
            CustomUndoRecordBO lastBO = bos.get(bos.size() - 1);
            Object afterImage = lastBO.getAfterImage();
            Map dbData = idAndPOMap.get(entry.getKey());
            // 取最后一次操作的类型，
            switch (lastBO.getOpeType()) {
                // 判断是否有脏写
                case INSERT:
                case UPDATE:
                    if (!Objects.equals(MongoQueryUtil.getMongoDataVersion(dbData), MongoQueryUtil.getMongoDataVersion(afterImage))) {
                        dirtyWriteStrategy.consume(xid, branchId, dbData, bos);
                    }
                    break;
                case DELETE:
                    if (dbData != null) {
                        dirtyWriteStrategy.consume(xid, branchId, dbData, bos);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("不应该出现的类型" + lastBO.getOpeType());
            }

            switch (firstBO.getOpeType()) {
                case INSERT:
                    if (firstBO.getBeforeImage() == null) {
                        deleteList.add(firstBO);
                    } else {
                        updateList.add(firstBO);
                    }
                    break;
                case UPDATE:
                case DELETE:
                    if (dbData == null) {
                        saveList.add(firstBO);
                    } else {
                        updateList.add(firstBO);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("不应该出现的类型" + firstBO.getOpeType());
            }
        }

        rollbackData(collectionName, saveList, updateList, deleteList);
    }

    /**
     * 处理三个集合，写入前镜像
     * @param collectionName
     * @param saveList
     * @param updateList
     * @param deleteList
     */
    private void rollbackData(String collectionName, List<CustomUndoRecordBO> saveList, List<CustomUndoRecordBO> updateList, List<CustomUndoRecordBO> deleteList) {
        if (!CollectionUtils.isEmpty(deleteList)) {
            Set<String> ids = deleteList.stream().map(CustomUndoRecordBO::getId).collect(Collectors.toSet());
            mongoTemplate.remove(buildInQuery(ids), collectionName);

        }
        if (!CollectionUtils.isEmpty(saveList)) {
            List<Map> beforeImagesList = saveList.stream().map(CustomUndoRecordBO::getBeforeImage).collect(Collectors.toList());
            mongoTemplate.insert(beforeImagesList, collectionName);
        }
        if (!CollectionUtils.isEmpty(updateList)) {
            for (CustomUndoRecordBO customUndoRecordBO : updateList) {
                mongoTemplate.updateFirst(
                    new Query(Criteria.where("id").is(customUndoRecordBO.getId()))
                    , MongoQueryUtil.updateGenerator(customUndoRecordBO.getBeforeImage())
                    , collectionName
                );
            }
        }

        // 新增或删除的数据组合成update告诉es去同步
        if (!CollectionUtils.isEmpty(saveList) || !CollectionUtils.isEmpty(updateList)) {
            List<CustomUndoRecordBO> bos = new ArrayList<>();
            bos.addAll(saveList);
            bos.addAll(updateList);
        }
    }


    private Query buildInQuery(Set<String> ids) {
        return new Query(Criteria.where("id").in(ids));
    }
}
