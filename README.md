# CHIME/PulsaR Processing Pipeline (CHIRPP)

A pipeline for processing fold-mode CHIME/Pulsar data on Fir.

`CHIRPP` makes use of PSRCHIVE and related routines to gather all available data for the chosen pulsar in the designated place and automatically perform checks for data consistency and exclude anomolous files, followed by a series of processing steps. Final products include RFI-cleaned, beamweighted profiles, a smoothed standard template profile, and a TOA (`.tim`) file.

Functionality to process new data and add the results to those of a previous run is under construction.

## Getting Started

In order to run successfully, `CHIRPP` requires several environment modules be loaded. What follows are the standard steps for constructing a clean environment and loading the necessary modules.

On `fir`, change to a directory where you want to install the environment. Your home directory should suffice. Then:
```
$ mkdir astro-work
$ cd astro-work
$ module load scipy-stack
$ python -m venv --prompt="astro-work" .venv
$ source .venv/bin/activate
```
Add this block to your `~/.bashrc` file:
```
alias astro-work="cd ~/astro-work; source .venv/bin/activate; cd"
alias load_pulsarStack="module use /project/6004902/chimepsr-software/v2/environment-modules; \
            module load pint-pal; \
            module load chime-psr; \
            module load psrchive; \
            module load chime-beam; \
            module load clfd"
```
You can then run `astro-work` and `load_pulsarStack` to load all the necessary modules. If you would like this to happen automatically upon log-in, add them to your `~/.bash_profile`.

Also add this line to your `~/.bash_profile`:
```
. env_parallel.bash
```
Once you have run `astro-work` and `load_pulsarStack`, run:
```
$ parallel --record-env
```

This records your current environment for GNU Parallel so your SLURM jobs keep all the environments you have loaded. You only need to run this once.

## Installation
```
$ git clone https://github.com/CHIME-Pulsar-Timing/CHIRPP.git
```
For convenience, you might also wish to add `/path/to/CHIRPP/src/CHIRPP` to your `$PATH` (where `/path/to/` is the directory in which you ran the above command). Then you will be able to run this package's scripts from the command line without any leading path.

## Running the Pipeline
Create a directory in your `~/project/` or `~/scratch` space to work in and decide what pulsar you want to process. `cd` into your directory. Then, it is as simple as running:
```
$ new_pulsar.py J1234+5678
```
The pulsar name you give must correspond to the name used by CHIME. e.g. not `J1939+2134` or `J0636+5128`, but `B1937+21` or `J0636+5129`.

You can find the full range of optional flags and their explanations with `new_pulsar.py -h`. In particular, you may wish to use:
```
-e $your_email  # Get email alerts when each step completes (or if one fails).
--skip $step    # Skip to the provided pipeline step in case you need to start from the middle.
--par $par      # Use the provided .par file.
-f              # Do not pause the pipeline to wait for manual quality checks.
--rmtar         # Automatically remove tarballs containing older data once they have been unpacked.
```
