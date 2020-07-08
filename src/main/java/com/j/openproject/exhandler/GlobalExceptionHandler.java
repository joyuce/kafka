package com.j.openproject.exhandler;

import java.util.List;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;

import org.springframework.validation.ObjectError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

import com.j.openproject.exception.AppException;
import com.j.openproject.model.base.CommonRs;
import com.j.openproject.model.code.CommonRsCode;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Joyuce
 * @Type ExceptionHandler
 * @Desc 控制层异常捕获处理器
 * @date 2019年11月21日
 * @Version V1.0
 */
@Slf4j
@ControllerAdvice
public class GlobalExceptionHandler {

    @SuppressWarnings("unchecked")
    @ResponseBody
    @ExceptionHandler(value = MethodArgumentNotValidException.class)
    public CommonRs notValidExceptionHandler(MethodArgumentNotValidException e) {
        CommonRs rs = CommonRs.createWithCode(CommonRsCode.VALID_ERROR);
        rs.setData(getErrorMsg(e.getBindingResult().getAllErrors()));
        return rs;
    }

    @ResponseBody
    @ExceptionHandler(value = Exception.class)
    public CommonRs exceptionHandler(Exception e) {
        log.error("捕获到未知Exception异常", e);
        return CommonRs.createWithCode(CommonRsCode.INT_ERROR);
    }


    @ResponseBody
    @ExceptionHandler(value = AppException.class)
    public CommonRs appExceptionHandler(AppException e) {
        log.info(new StringBuilder().append("业务异常 code:").append(e.getResultCode().getCode()).append(" msg:")
                .append(e.getResultCode().getCnMsg()).toString());
        return CommonRs.createWithCode(e.getResultCode());
    }

    @ResponseBody
    @ExceptionHandler(value = NullPointerException.class)
    public CommonRs nullExceptionHandler(NullPointerException e) {
        log.error("捕获到空指针异常", e);
        return CommonRs.createWithCode(CommonRsCode.INT_ERROR);
    }


    private String getErrorMsg(List<ObjectError> allErrors) {
        StringBuilder message = new StringBuilder();
        for (ObjectError error : allErrors) {
            message.append(error.getDefaultMessage()).append(" & ");
        }
        return message.substring(0, message.length() - 3);
    }

    /**
     * 参数校验异常
     *
     * @param e
     * @return
     */
    @SuppressWarnings("unchecked")
    @ExceptionHandler(value = ConstraintViolationException.class)
    @ResponseBody
    public CommonRs handleConstraintViolationException(ConstraintViolationException e) {
        CommonRs rs = CommonRs.createWithCode(CommonRsCode.VALID_ERROR);
        rs.setData(getErrorMsgByEx(e));
        return rs;
    }

    /**
     * 参数校验错误信息处理
     *
     * @param exception
     * @return
     */
    private String getErrorMsgByEx(ConstraintViolationException exception) {
        Set<ConstraintViolation<?>> violations = exception.getConstraintViolations();
        StringBuilder builder = new StringBuilder();
        for (ConstraintViolation violation : violations) {
            builder.append(violation.getMessage());
        }
        return builder.toString();
    }


}
