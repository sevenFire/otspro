package com.baosight.xinsight.ots.rest.controller;


import com.baosight.xinsight.model.PermissionCheckUserInfo;
import com.baosight.xinsight.ots.rest.model.operate.ErrorMode;
import com.baosight.xinsight.ots.rest.util.PermissionUtil;
import com.baosight.xinsight.ots.rest.util.RegexUtil;

import org.apache.log4j.Logger;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * @author liyuhui
 * @date 2018/12/13
 * @description
 */
@Path("/table")
public class TableController {
    private static final Logger LOG = Logger.getLogger(TableController.class);

    /**
     * 创建表
     * @param tableName
     * @param body
     * @return
     */
    @POST
    @Path("/{tablename}")
    @Consumes({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response post(@PathParam("tablename") String tableName, String body) {
        //todo lyh 校验表名合法性

        //获得userInfo和body
        PermissionCheckUserInfo userInfo = new PermissionCheckUserInfo();
        userInfo = PermissionUtil.getUserInfoModel(userInfo, request);
        LOG.debug("Post:" + tableName + "\nContent:\n" + body);



    }

}
