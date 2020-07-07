# -*- coding: utf-8 -*-
"""
文档说明：
    ags系统的配置文件存放地点 路径~/ags/profile/
    本文档包含敏感信息
    gitignore中将此目录设置为不上传
    如文档移植，请复制profile目录，否则会报错
    引用规范：请使用下列语句
        from setup import setup
        from setup import smtp_setup as setup
    调用规范：
        调用时可直接使用setup.month 基类与子类可以保持一致（否则子类需要使用smtp_setup.month，不利于保持代码一致性）
        或，set=setup(month='2018-08')
            month=set.month

基本框架：
    class setup 配置文件的基类（待移植）


版本信息：
    version 0.1
    乔晖 2020/6/8

修改日志：
    2020/6/8
        1. 新建框架
"""
import pandas as pd