# Evaluation of paper _Timestamp as a Service, not an Oracle_

We forked [tikv/pd](https://github.com/tikv/pd) from commit [`19f7dd9`](https://github.com/tikv/pd/commit/19f7dd98b087a7435fd63d8f38752ee1b3992cbb),
and injected the TaaS algorithm into the codebase to compare the performance.

## Build and run
Tested with Go version 1.20.3:
```shell
# Compile the client and servers
make

# Launch the servers (that serve both TaaS and TiDB requests)
make pd

# Run the TaaS client
make test-taas

# Run the TiDB client
make test-tidb
```

## Read the logs
### TaaS
Example log entry:
> [2023/04/22 13:34:53.116 +00:00] [INFO] [main.go:399] ["312541 5154600750196961478"]

The conclusion timestamp is `312541 5154600750196961478`, where:
- The higher bits `312541` represent the logical counter.
- The lower bits `5154600750196961478` identify the responding server.

### TiDB-PD
Example log entry:
> [2023/04/22 14:09:56.238 +00:00] [INFO] [main.go:399] ["1682143796231 35"]

The conclusion timestamp is `1682143796231 35`, where:
- The higher bits `1682143796231` come from the physical timer.
- The lower bits `35` represent the logical counter within each millisecond.

## Tamper the cluster
Running `make pd` prints the process identifiers of the five servers in order, e.g.:
> 94519
95167
95497
95812
96128

To kill a process, specify its corresponding identifier.  For the previous example:
```shell
kill 94519 # Server 1
kill 95167 # Server 2
```

You can also replace the killed servers with new machines, e.g.:
```shell
make pd_1 pd_2 # Resume Servers 1 and 2
```

The process identifiers are listed numerically, which in most cases correspond
to the order in which the servers are created.  To determine the exact identifer
of each server process, uncomment the `# pgrep pd-server` command in the `pd_%`
target of the `Makefile`.

## Finish and cleanup
```shell
make cl
```
Expect `pgrep pd-server` to print empty results.
If the `pd-server` processes aren't purged, try re-running `pkill pd-server`.

## Navigate through the lines
Our pseudocode in the paper corresponds to the codebase as follows.

### Client side
- The core mechanism of the TaaS `client()` on Page 4 Lines 407--434 (Figure 4 Lines 1--27)
  maps to `client/taas_dispatcher.go` as the `dispatchRequest` function.
- The `snd_rcv` function on Page 4 Lines 445--440 (Figure 4 Lines 39--43)
  maps to `client/taas_dispatcher.go` as the `sndAndRcv` function.
- The `update` function on Page 4 Lines 435--443 (Figure 4 Lines 29--37)
  is to provide a clean, functional-style abstraction of state transition.
  In this codebase:
  + The session data is updated right before determining the candidate in `dispatchRequest`,
    just like that shown in the pseudocode;
  + The cache data is updated within the asynchronous `sndAndRcv` routine,
    before inserting the response into the event channel,
    unlike the pseudocode that updates the cache after taking the response out of the event channel.
    We believe that the earlier the cache gets updated, the earlier the sessions might conclude.

### Server side
- The upper-bound-reservation mechanism of the TaaS server on Page 7 Lines 715--718 (Figure 8 Lines 19-22)
  maps to `pkg/tso/taas.go` as the `reserveTaasLimit` function.
- The `Tick()` function on Page 7 Lines 697--709 (Figure 8 Lines 1--12)
  maps to `pkg/tso/taas.go` as the `generateTaasTSO` function,
  with the blocking and non-blocking reservation logic defined in the `setTaasHigh` function.
- The failure recovery `init()` function on Page 7 Lines 710--714 (Figure 8 Lines 14--17)
  maps to `pkg/tso/taas.go` as the `Initialize` function,
  with the blocking reservation done by calling the `setTaasHigh` function.
