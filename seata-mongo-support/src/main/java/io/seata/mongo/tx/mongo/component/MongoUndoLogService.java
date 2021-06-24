package io.seata.mongo.tx.mongo.component;

import io.seata.mongo.tx.mongo.bo.CustomUndoRecordBO;
import io.seata.mongo.tx.mongo.component.MongoUndoLogOpeService.State;
import io.seata.mongo.tx.mongo.util.DirtyWriteStrategy;
import io.seata.mongo.tx.mongo.util.UndoLogConsumer;
import io.seata.mongo.tx.support.MongoSeataRmHelper;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.CollectionUtils;

/**
 * @description: 管理mongo的undo log
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/18 14:58
 * @version:v1.0
 */
@Slf4j
@Service
public class MongoUndoLogService {

    @Resource
    private MongoSeataRmHelper mongoSeataRmHelper;
    @Resource
    private DirtyWriteStrategy dirtyWriteStrategy;
    @Resource
    private MongoTemplate mongoTemplate;
    @Resource(name = "plainMongoTransactionTemplate")
    private TransactionTemplate plainMongoTransactionTemplate;
    // @Resource
    // private EsSyncPushMessageSender esSyncPushMessageSender;
    @Resource
    private MongoUndoLogOpeService mongoUndoLogOpeService;

    /**
     * 一阶段提交时写入undolog到redis
     *
     * @param xid
     */
    public void flushUndoLogs(String xid) {
        Long branchId = mongoSeataRmHelper.getContext().get().getBranchId();
        Collection<CustomUndoRecordBO> tempUndoLog = mongoSeataRmHelper.getTempUndoLog(xid);
        if (CollectionUtils.isEmpty(tempUndoLog)) {
            return;
        }

        mongoUndoLogOpeService.writeToMongo(xid, branchId, tempUndoLog, State.Normal);

        // 清除本地的undolog
        mongoSeataRmHelper.removeTempUndoLog(xid);
    }

    /**
     * 处理回滚
     *
     * @param xid
     * @param branchId
     */
    public void undo(String xid, long branchId) {
        UndoLogConsumer undoLogConsumer = new UndoLogConsumer(mongoTemplate, dirtyWriteStrategy, xid, branchId);

        // 在mongo事务中消费undolog
        plainMongoTransactionTemplate.executeWithoutResult(s -> {
            List<CustomUndoRecordBO> customUndoRecordBOS = mongoUndoLogOpeService.findAndRemoveFromMongo(xid, branchId);
            if (CollectionUtils.isEmpty(customUndoRecordBOS)) {
                return;
            }

            // 将undolog按照collectionName分组并依次消费
            Map<String, List<CustomUndoRecordBO>> collect = customUndoRecordBOS.stream().collect(Collectors.groupingBy(CustomUndoRecordBO::getCollectionName));

            for (Entry<String, List<CustomUndoRecordBO>> entry : collect.entrySet()) {
                undoLogConsumer.accept(entry.getKey(), entry.getValue());
            }
        });
    }

    /**
     * 删除指定日期之前的undolog
     *
     * @param logCreated
     */
    public void deleteUndoLogByLogCreated(Date logCreated) {
        mongoUndoLogOpeService.deleteUndoLogByLogCreated(logCreated);
    }

    public void batchDeleteUndoLog(Set<String> xids, Set<Long> branchIds) {
        mongoUndoLogOpeService.batchDeleteUndoLog(xids, branchIds);
    }
}
