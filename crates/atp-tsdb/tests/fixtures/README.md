# TSDB test fixtures

`index_format_v1` and `testblock` are copied from the MIT-licensed `tsdb` crate
v0.3.3 (`testdata/`), used to exercise the native index/chunk parser against real
Prometheus blocks. `testblock` is a real 35,354-series block with 5.55 million
samples; `index_format_v1` has 102 series in index format v1. They are test data
only and are never written by the converter.

`native_hist` is the upstream native-histogram fixture (histogram chunks that
must be reported as unrepresentable as a single float column).
