# Evaluation of paper _Timestamp as a Service, not an Oracle_

We forked [tikv/pd](https://github.com/tikv/pd) from commit [`19f7dd9`](https://github.com/tikv/pd/commit/19f7dd98b087a7435fd63d8f38752ee1b3992cbb),
and injected the TaaS algorithm into the codebase to compare their performance.

Our pseudocode in the paper corresponds to the codebase as follows:

- The core mechanism of the TaaS `client()` on Page 5 (Figure 4 Lines 3--31)
  maps to `client/taas_dispatcher.go` as the `dispatchRequest` function.
- The `snd_rcv` function on Page 5 (Figure 4 Lines 33--38)
  maps to `client/taas_dispatcher.go` as the `sndAndRcv` function.
- The `update` function on Page 5 (Figure 4 Lines 40--44)
  is to provide a clean, functional-style abstraction of state transition.
  In this codebase:
  + The session data is updated right before determining the candidate in `dispatchRequest`;
  + The cache data is updated within the asynchronous `sndAndRcv` routine,
    before inserting the response into the event channel.