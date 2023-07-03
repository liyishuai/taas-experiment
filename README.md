# Evaluation of paper _Timestamp as a Service, not an Oracle_

We forked [tikv/pd](https://github.com/tikv/pd) from commit [`19f7dd9`](https://github.com/tikv/pd/commit/19f7dd98b087a7435fd63d8f38752ee1b3992cbb),
and injected the TaaS algorithm into the codebase to compare their performance.

Our pseudocode in the paper corresponds to the codebase as follows.

### Client side
- The core mechanism of the TaaS `client()` on Page 5 (Figure 4 Lines 1--27)
  maps to `client/taas_dispatcher.go` as the `dispatchRequest` function.
- The `snd_rcv` function on Page 5 (Figure 4 Lines 39--43)
  maps to `client/taas_dispatcher.go` as the `sndAndRcv` function.
- The `update` function on Page 5 (Figure 4 Lines 29--37)
  is to provide a clean, functional-style abstraction of state transition.
  In this codebase:
  + The session data is updated right before determining the candidate in `dispatchRequest`,
    just like that shown in the pseudocode;
  + The cache data is updated within the asynchronous `sndAndRcv` routine,
    before inserting the response into the event channel,
    unlike the pseudocode that updates the cache after taking the response out of the event channel.
    We believe that the earlier the cache gets updated, the earlier the sessions might conclude.

### Server side
- The timestamp allocation mechanism of the TaaS server on Page 11 (Figure 12 Lines 14--17)
  maps to `pkg/tso/taas.go` as the `reserveTaasLimit` function.
- The `Tick()` function on Page 11 (Figure 12 Lines 1--12)
  maps to `pkg/tso/taas.go` as the `generateTaasTSO` function,
  with the timestamp allocation logic defined in the `setTaasHigh` function.
- The `recovery()` function on Page 11 (Figure 8 Lines 19--22)
  maps to `pkg/tso/taas.go` as the `Initialize` function.
