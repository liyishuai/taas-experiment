pd-simulator
========

pd-simulator is a tool to reproduce some scenarios and evaluate the schedulers' efficiency.

## Build
1. [Go](https://golang.org/) Version 1.9 or later
2. In the root directory of the [PD project](https://github.com/tikv/pd), use the `make simulator` command to compile and generate `bin/pd-simulator`


## Usage

This section describes how to use the simulator.

### Flags description

```
-pd string
      Specify a PD address (if this parameter is not set, it will start a PD server from the simulator inside)
-config string
      Specify a configuration file for the PD simulator
-case string
      Specify the case which the simulator is going to run
-serverLogLevel string
      Specify the PD server log level (default: "fatal")
-simLogLevel string
      Specify the simulator log level (default: "fatal")
```

Run all cases:

    ./pd-simulator

Run a specific case with an internal PD:

    ./pd-simulator -case="casename"

Run a specific case with an external PD:

    ./pd-simulator -pd="http://127.0.0.1:2379" -case="casename"
