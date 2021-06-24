package io.seata.mongo.tx.support;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.seata.mongo.tx.mongo.bo.CustomUndoRecordBO;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

/**
 * @description: 管理seata分支和context
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/17 17:00
 * @version:v1.0
 */
@Slf4j
public class MongoSeataRmHelper extends BaseStorableSeataRmHelper {

    /**
     * 根据xid存储undolog
     * Multimap<mongoXid, undologs>
     */
    private final ThreadLocal<Multimap<String, CustomUndoRecordBO>> MONGO_UNDO_RECORDS_THREAD_LOCAL = ThreadLocal.withInitial(ArrayListMultimap::create);
    private ThreadLocal<ConnectionContext> context = ThreadLocal.withInitial(ConnectionContext::new);

    public MongoSeataRmHelper(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public ThreadLocal<Multimap<String, CustomUndoRecordBO>> getUndoLogThreadLocal() {
        return MONGO_UNDO_RECORDS_THREAD_LOCAL;
    }

    @Override
    public ThreadLocal<ConnectionContext> getContext() {
        return context;
    }


}
