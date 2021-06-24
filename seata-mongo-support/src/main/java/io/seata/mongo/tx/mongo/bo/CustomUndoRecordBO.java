
package io.seata.mongo.tx.mongo.bo;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The type mongo records.
 *
 * @author ninggc
 * @see io.seata.rm.datasource.sql.struct.TableRecords
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class CustomUndoRecordBO implements Serializable {

    private static final long serialVersionUID = 7633554683628381093L;

    /**
     * mongo集合名称
     */
    private String collectionName;

    /**
     * mongo数据id
     */
    private String id;

    /**
     * 数据操作类型
     */
    private OpeType opeType;

    /**
     * 数据操作时间
     */
    private LocalDateTime opeTime;

    /**
     * 前镜像
     */
    private Map beforeImage;
    /**
     * 后镜像
     */
    private Map afterImage;

    public enum OpeType {
        /**
         * 查询
         */
        // SELECT,
        /**
         * 新增
         */
        INSERT,
        /**
         * 更新
         */
        UPDATE,
        /**
         * 删除
         */
        DELETE,
        ;
    }

}
