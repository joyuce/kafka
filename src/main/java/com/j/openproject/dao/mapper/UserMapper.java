package com.j.openproject.dao.mapper;

import org.apache.ibatis.annotations.Mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.j.openproject.dao.entity.User;

/**
 * @author Joyuce
 * @Type UserMapper
 * @Desc
 * @date 2019年11月22日
 * @Version V1.0
 */
@Mapper
public interface UserMapper extends BaseMapper<User> {

    public User getTheList();

}
