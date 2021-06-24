package io.seata.mongo.tx.support;

import io.seata.mongo.tx.mongo.bo.CustomUndoRecordBO;
import java.util.Collection;

/**
 * @description: 本地的undolog临时保存接口
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/17 18:11
 * @version:v1.0
 */
public interface UndoLogStorable {
    /**
     * 保存undolog在本地
     * @param xid
     * @param customUndoRecordBOS
     * @param collectionName
     */
    void tempSaveUndoLog(String xid, Collection<CustomUndoRecordBO> customUndoRecordBOS, String collectionName);

    /**
     * 获取本地保存的undolog
     * @param xid
     * @return
     */
    Collection<CustomUndoRecordBO> getTempUndoLog(String xid);

    /**
     * 清理本地的undolog
     * @param xid
     */
    void removeTempUndoLog(String xid);

}
