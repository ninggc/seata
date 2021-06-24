package io.seata.mongo.tx.mongo.bo;

import java.time.LocalDateTime;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * @description: mongo数据库中存储的undolog记录
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/23 20:08
 * @version:v1.0
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class UndoLogPO {
    @Id
    private String id;
    @Field("xid")
    private String xid;
    @Field("branch_id")
    private Long branchId;
    @Field("log_status")
    private Integer logStatus;
    @Field("context")
    private String context;
    @Field("rollback_info")
    private List<CustomUndoRecordBO> rollbackInfo;
    @Field("log_created")
    private LocalDateTime logCreated;
    @Field("log_modified")
    private LocalDateTime logModified;
}
