package io.seata.mongo.tx.mongo.component;

import io.seata.mongo.tx.mongo.bo.CustomUndoRecordBO;
import io.seata.mongo.tx.mongo.bo.UndoLogPO;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

/**
 * @description: 到mongodb中读写undolog
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/21 18:20
 * @version:v1.0
 */
@Service
public class MongoUndoLogOpeService {

    public final String mongoUndoLogTable = "undo_log";
    public final String columnXid = "xid";
    public final String columnBranchId = "branch_id";
    public final String columnLogStatus = "log_status";
    public final String columnContext = "context";
    public final String columnRollbackInfo = "rollback_info";
    public final String columnLogCreated = "log_created";
    public final String columnLogModified = "log_modified";

    private final MongoTemplate mongoTemplate;

    public MongoUndoLogOpeService(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    /**
     * 将undolog写入mongo
     *
     * @param xid
     * @param branchId
     * @param customUndoRecordBOs
     * @param logStatus
     * @return
     */
    public UndoLogPO writeToMongo(String xid, long branchId, Collection<CustomUndoRecordBO> customUndoRecordBOs, State logStatus) {
        UndoLogPO undoLogPO = new UndoLogPO(null, xid, branchId, logStatus.getValue(), null
            , new ArrayList<>(customUndoRecordBOs), LocalDateTime.now(), LocalDateTime.now());
        return mongoTemplate.insert(undoLogPO, mongoUndoLogTable);
    }

    /**
     * 从mongo中读取undolog
     *
     * @param xid
     * @param branchId
     * @return
     */
    public List<CustomUndoRecordBO> findAndRemoveFromMongo(String xid, Long branchId) {
        Query normalQuery = new Query(Criteria.where(columnXid).is(xid)
            .and(columnBranchId).is(branchId));
        UndoLogPO undoLogPO = mongoTemplate.findOne(normalQuery, UndoLogPO.class, mongoUndoLogTable);

        if (undoLogPO == null) {
            // 没有undolog，可能对应的分支还未提交，防御性的插入state=1的undolog，阻止对应分支提交
            writeToMongo(xid, branchId, Collections.emptyList(), State.GlobalFinished);
            return Collections.emptyList();
        }

        if (Integer.valueOf(State.GlobalFinished.getValue()).equals(undoLogPO.getLogStatus())) {
            // 非NORMAL undolog，不处理
            return Collections.emptyList();
        }

        mongoTemplate.remove(new Query(Criteria.where("id").is(undoLogPO.getId())), mongoUndoLogTable);
        return undoLogPO.getRollbackInfo();
    }

    /**
     * 从mongo中删除undolog
     *
     * @param xid
     * @param branchId
     */
    public void deleteFromMongo(String xid, Long branchId) {
        Criteria criteria = Criteria.where(columnXid).is(xid)
            .and(columnBranchId).is(branchId)
            .and(columnLogStatus).is(State.Normal);

        mongoTemplate.remove(new Query(criteria), mongoUndoLogTable);
    }

    /**
     * 定时删除（默认7）7天前的undolog
     *
     * @param logCreated
     */
    public void deleteUndoLogByLogCreated(Date logCreated) {
        LocalDateTime localDateTime = logCreated.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();

        Criteria criteria = Criteria.where(columnLogCreated).lt(localDateTime);
        mongoTemplate.remove(new Query(criteria), mongoUndoLogTable);
    }

    /**
     * 批量删除
     *
     * @param xids
     * @param branchIds
     */
    public void batchDeleteUndoLog(Set<String> xids, Set<Long> branchIds) {
        Criteria criteria = Criteria.where(columnXid).in(xids).orOperator(
            Criteria.where(columnBranchId).in(branchIds)
        );

        mongoTemplate.remove(new Query(criteria), mongoUndoLogTable);
    }

    public enum State {
        /**
         * This state can be properly rolled back by services
         */
        Normal(0),
        /**
         * This state prevents the branch transaction from inserting undo_log after the global transaction is rolled
         * back.
         */
        GlobalFinished(1);

        private int value;

        State(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }
}
