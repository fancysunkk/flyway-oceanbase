# 使用说明

## 背景介绍

项目需要使用springboot2.6.8+jdk8，而flyway的社区版早已停止对springboot2的支持。同时为了满足信创要求，我司正在考虑现有业务数据库由mysql向OceanBase迁移，迁移过程需要解决flyway的兼容性，查询社区发现flyway最新已有提供对于OceanBase的支持，
但是支持spring版本为3+，于是只能考虑修改代码以支持现有环境

## 使用方法

引入本依赖同时引入flyway-core:8.0.5后，正常使用即可

```
<dependency>
    <groupId>org.flywaydb</groupId>
    <artifactId>flyway-core</artifactId>
</dependency>
<dependency>
    <groupId>io.github.fancysunkk</groupId>
    <artifactId>flyway-oceanbase</artifactId>
    <version>1.1.0</version>
</dependency>
```

