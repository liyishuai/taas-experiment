# Run TSO bench in PD
### 编译
```
make
```

### 本地多节点PD server
** 启动 **
```
make pd
```

** 停止 **
```
make cl
```

### 启动本地 tso-bench
```
make bench
```

默认运行一分钟,结果保存在tso_bench.log中,查看最终统计结果
```
grep -A 4 "Total:" ./tso_bench.log
```
