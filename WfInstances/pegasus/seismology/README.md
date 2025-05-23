<img src="https://wfcommons.org/images/wfcommons-horizontal.png" width="350" />
<img src="https://pegasus.isi.edu/documentation/_static/pegasus_circular_white_logo.png" width="100"/>

# Execution Instances for Seismic Cross Correlation Workflow

## Workflow Description

The Seismic Cross Correlation workflow represents a common data-intensive
analysis pattern used by many seismologists. The workflow preprocesses and
cross-correlates instances (sequences of measurements of acceleration in three
dimensions) from multiple seismic stations.
 The workflow is composed of two different tasks:

  1. `sG1IterDecon`: receives a pair of signals, one from EGF and another
     from MShock, and computes seismogram deconvolutions to estimate
     earthquake source time functions (STFs). The output file is in the SAC
     (Seismic Analysis Code) format.
  2. `siftSTFByMisfit`: receives all STFs generated from the sG1IterDecon jobs
     and sifts the data by misfit. Only the good fits are kept and compressed
     into a single `.tar.gz` file.

The figure below shows an overview of the Seismic Cross Correlation workflow structure:

<img src="docs/images/seismology.png?raw=true" width="350">

### Research Publication

Details of the Seismic Cross Correlation workflow description, computational
jobs, and performance metrics can be found in the following research
publication:

- Filgueira, R., Ferreira da Silva, R., Krause, A., Deelman, E., & Atkinson,
  M. (2016). Asterism: Pegasus and dispel4py hybrid workflows for data-intensive
  science. In 7th International Workshop on Data-Intensive Computing in the
  Clouds (pp. 1–8). https://doi.org/10.1109/DataCloud.2016.004

## Execution Instances

Execution instances are formatted according to the
[WfCommons JSON format](https://github.com/wfcommons/workflow-schema)
for describing workflow executions. Execution instances from different
computing platforms are organized into sub-directories.

Instance files are named using the following convention:
`seismology-<COMPUTE_PLATFORM>-<NUM_PAIRS>p-<RUN_ID>.json`, where:

- `<COMPUTE_PLATFORM>`: The compute platform where the actual Pegasus workflow
  was executed (e.g., `chameleon`).
- `<NUM_PAIRS>`: The number of pair of signals to estimate earthquake STFs.
- `<RUN_ID>`: The workflow execution identification.

### Workflow Structure

The Seismic Cross Correlation workflow structure depends exclusively on the
_number of pairs_ (`<NUM_PAIRS>`), in which each pair will be processed by a
`sG1IterDecon` job. The workflow has a single `siftSTFByMisfit` (regardless
the number of pairs) that merges and processess all STFs generated by the
first job.
