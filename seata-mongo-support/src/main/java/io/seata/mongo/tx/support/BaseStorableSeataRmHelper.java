package io.seata.mongo.tx.support;

import com.google.common.collect.Multimap;
import io.seata.mongo.tx.mongo.bo.CustomUndoRecordBO;
import java.util.Collection;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

/**
 * @description: 管理seata分支和context
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/17 17:00
 * @version:v1.0
 */
@Slf4j
public abstract class BaseStorableSeataRmHelper extends AbstractSeataRmHelper {

    public BaseStorableSeataRmHelper(DataSource dataSource) {
        super(dataSource);
    }

    /**
     * undolog存储在本地的ThreadLocal
     * @return
     */
    public abstract ThreadLocal<Multimap<String, CustomUndoRecordBO>> getUndoLogThreadLocal();


    /**
     * db交互结束后清空tempUndoLog
     */
    public void doCleanAfterCompletion() {
        removeTempUndoLog(getContext().get().getXid());

        getContext().remove();
    }


    @Override
    public void tempSaveUndoLog(String xid, Collection<CustomUndoRecordBO> customUndoRecordBOS, String collectionName) {
        if (CollectionUtils.isEmpty(customUndoRecordBOS)) {
            return;
        }

        getUndoLogThreadLocal().get().putAll(xid, customUndoRecordBOS);
        buildLockKey(customUndoRecordBOS, collectionName);
    }

    @Override
    public Collection<CustomUndoRecordBO> getTempUndoLog(String xid) {
        return getUndoLogThreadLocal().get().get(xid);
    }

    @Override
    public void removeTempUndoLog(String xid) {
        getUndoLogThreadLocal().get().removeAll(xid);
    }

}
