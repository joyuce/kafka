package com.j.openproject.model.code;

import org.apache.commons.lang3.StringUtils;

import lombok.Getter;

/**
 * @author shenxiaodong
 * @Type ResultCode
 * @Desc 通过结果枚举
 * @date 2019年07月01日
 * @Version V1.0
 */
@Getter
public enum CommonRsCode implements ResultCode {

    /**
     * 处理成功
     */
    SUCCESS("0", "处理成功", "Successful processing"),

    /**
     * 接口异常
     */
    INT_ERROR("10000", "接口异常", "Interface exception"),

    /**
     * 处理失败
     */
    FAIL("10001", "处理失败", "Processing failure"),

    /**
     * 接口请求方式错误
     */
    REQUEST_ERROR("10002", "接口请求方式错误", "Interface request mode error"),

    /**
     * 参数校验不通过
     */
    VALID_ERROR("10003", "参数校验不通过", "Parameter verification does not pass"),

    /**
     * 权限校验不通过
     */
    AUTH_CHECK_ERROR("10004", "权限校验不通过", "Permission check failed"),
    ;

    /**
     * 错误码
     */
    private String code;

    /**
     * 中文信息
     */
    private String cnMsg;

    /**
     * 英文信息
     */
    private String enMsg;

    private CommonRsCode(String code, String cnMsg, String enMsg) {
        this.code = code;
        this.cnMsg = cnMsg;
        this.enMsg = enMsg;
    }

    /**
     * 通过name获取结果码
     *
     * @param code 错误码
     * @return 返回业务结果码
     */
    public static CommonRsCode getResultEnum(String code) {
        for (CommonRsCode result : values()) {
            if (StringUtils.equals(result.getCode(), code)) {
                return result;
            }
        }
        return null;
    }

    @Override
    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    @Override
    public String getCnMsg() {
        return cnMsg;
    }

    public void setCnMsg(String cnMsg) {
        this.cnMsg = cnMsg;
    }

    @Override
    public String getEnMsg() {
        return enMsg;
    }

    public void setEnMsg(String enMsg) {
        this.enMsg = enMsg;
    }
}
