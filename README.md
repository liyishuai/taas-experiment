# File-to-Pseudo Mapping
|Peseudo in paper|File|Function|
|-|-|-|
|line 407-434|client/taas_dispatcher.go| dispatchRequest|
|line 435-443|client/taas_dispatcher.go| "inline implementation"|
|line 445-449|client/taas_dispatcher.go| sndAndRcv|
|line 697-709|pkg/tso/taas.go|generateTaasTSO|
|line 710-714|pkg/tso/taas.go|Initialize|
|line 715-718|pkg/tso/taas.go|reserveTaasLimit|


# Run TSO bench in PD
### 编译
```
make
```

### 本地多节点PD server
**启动**
```
make pd
```

**停止**
```
make cl
```

### 启动本地 tso-bench

**启动tidb-pd tso的测试**
```
make global
```

**启动taas的测试**
```
make taas
```

默认运行一分钟,结果保存在tso_bench.log中,查看最终统计结果
```
grep -A 4 "Total:" ./tso_bench.log
```
