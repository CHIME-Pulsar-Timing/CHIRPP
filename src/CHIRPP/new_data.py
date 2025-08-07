#!/usr/bin/env python

import argparse
import subprocess
import os
import numpy as np
from datetime import datetime
from CHIRPP_utils import *


current_dir = subprocess.check_output("pwd", shell=True, text=True).strip("\n")

pipeline_steps = np.array([
    "ephemNconvert",
    "clean5G",
    "clean",
    "beamWeight",
    "scrunch",
    "tim",
])

parser = argparse.ArgumentParser(
    description="Generate template profile and times of arrival for new pulsar dataset."
)
parser.add_argument("pulsar", type=str, help="Pulsar name, e.g. J0437-4715")
parser.add_argument(
    "-e",
    "--email",
    type=str,
    help="Email to be alerted when jobs complete or fail.",
)
parser.add_argument(
    "-t",
    "--tim",
    type=str,
    help="Path to this pulsar's tim file."
)
parser.add_argument(
    "-d",
    "--data_directory",
    type=str,
    default=current_dir,
    help="Path to copy the archives to (or where they already live if using --skip; current directory by default).",
)
parser.add_argument(
    "--skip", choices=pipeline_steps, type=str, help="Skip to the specified step."
)
parser.add_argument(
    "--tjob_paramcheck",
    type=str,
    default="3:00:00",
    help="Time allotted to newParamCheck.sh job, HH:MM:SS (3h by default).",
)
parser.add_argument(
    "--tjob_ephemNconvert",
    type=str,
    default="3:00:00",
    help="Time allotted to parallel_ephemNconvert.sh job, HH:MM:SS (3h by default).",
)
parser.add_argument(
    "--tjob_clean5G",
    type=str,
    default="3:00:00",
    help="Time allotted to parallel_clean5G.sh job, HH:MM:SS (3h by default).",
)
parser.add_argument(
    "--tjob_clean",
    type=str,
    default="3:00:00",
    help="Time allotted to parallel_clean.sh job, HH:MM:SS (3h by default).",
)
parser.add_argument(
    "--tjob_beamweight",
    type=str,
    default="3:00:00",
    help="Time allotted to parallel_beamWeight.sh job, HH:MM:SS (3h by default).",
)
parser.add_argument(
    "--tjob_scrunch",
    type=str,
    default="3:00:00",
    help="Time allotted to parallel_scrunch.sh job, HH:MM:SS (3h by default).",
)
parser.add_argument(
    "--tjob_template",
    type=str,
    default="3:00:00",
    help="Time allotted to template_run.sh job, HH:MM:SS (3h by default).",
)
parser.add_argument(
    "--tjob_tim",
    type=str,
    default="3:00:00",
    help="Time allotted to tim_run.sh job, HH:MM:SS (3h by default).",
)

args = parser.parse_args()

email = parse_email(args.email)

pathcheck(args.data_directory)

today = datetime.today().strftime('%Y-%m-%d')

### To-do: copy bash scripts (+chime_zap.psh) or do rework

### To-do: find new data on Cedar

### To-do: store/locate sorted_paramList.txt from initial run

### To-do: store/locate config.sh from initial run

### To-do: store/locate template from initial run

if args.skip:
    try:
        skipnum = np.where(args.skip == pipeline_steps)[0][0]
    except IndexError:
        print(f"error: use a valid pipeline step with --skip, one of: {pipeline_steps}")
        exit(1)
else:
    skipnum = -1

if skipnum == -1:
    exp_paramcheck = [
            "Cut short subints, standardize nbin, freq, bw, nchan, npol",
            "Files that fail checks logged in ${PARAMETER}Fail/${PARAMETER}Fail"+f".{today}.log",
            "Adjust tjob with --tjob_paramcheck.",
        ]
    jobname_paramcheck = f"newParamCheck_{args.pulsar}_{today}"
    outfile_paramcheck = f"{jobname_paramcheck}.out"
    cmd_paramcheck = sbatch_cmd(
        "newParamCheck.sh",
        email,
        mem="66G",
        jobname=jobname_paramcheck,
        outfile=outfile_paramcheck,
        tjob=args.tjob_paramcheck,
    )
    outfile_paramcheck = my_cmd(
        cmd_paramcheck, exp_paramcheck, checkcomplete=outfile_paramcheck
    )

processingjob_base = sbatch_cmd(None, email, mem="12G")

if skipnum < 1:
    exp_ephemNconvert = [
        "Install ephemeris before averaging to ensure best data quality.",
        "Adjust tjob with --tjob_ephemNconvert",
    ]
    outfile_ephemNconvert = f"ephemNconvert_{args.pulsar}.out"
    cmd_ephemNconvert = f"{processingjob_base}--time={args.tjob_ephemNconvert} -J ephemNconvert_{args.pulsar} -o {outfile_ephemNconvert} ephemNconvert.sh"
    outfile_ephemNconvert = my_cmd(
        cmd_ephemNconvert, exp_ephemNconvert, checkcomplete=outfile_ephemNconvert
    )

if skipnum < 2:
    exp_clean5G = [
        "Zap known bad channels.",
        "Adjust tjob with --tjob_clean5G",
    ]
    outfile_clean5G = f"clean5G_{args.pulsar}.out"
    cmd_clean5G = f"{processingjob_base}--time={args.tjob_clean5G} -J clean5G_{args.pulsar} -o {outfile_clean5G} clean5G.sh"
    outfile_clean5G = my_cmd(cmd_clean5G, exp_clean5G, checkcomplete=outfile_clean5G)
    check_num_files(
        ".ar", ".zap", logfile=outfile_clean5G, force_proceed=args.force_proceed
    )

if skipnum < 3:
    exp_clean = [
        "Run clfd.",
        "Adjust tjob with --tjob_clean",
    ]
    outfile_clean = f"clean_{args.pulsar}.out"
    cmd_clean = f"{processingjob_base}--time={args.tjob_clean} -J clean_{args.pulsar}  -o {outfile_clean} clean.sh"
    outfile_clean = my_cmd(cmd_clean, exp_clean, checkcomplete=outfile_clean)
    check_num_files(
        ".zap", ".zap.clfd", logfile=outfile_clean, force_proceed=args.force_proceed
    )

if skipnum < 4:
    exp_beamWeight = [
        "Run beam weighting.",
        "Adjust tjob with --tjob_beamweight",
    ]
    outfile_beamWeight = f"beamWeight_{args.pulsar}.out"
    cmd_beamWeight = f"{processingjob_base}--time={args.tjob_beamweight} -J beamWeight_{args.pulsar} -o {outfile_beamWeight} beamWeight.sh"
    outfile_beamWeight = my_cmd(
        cmd_beamWeight, exp_beamWeight, checkcomplete=outfile_beamWeight
    )
    check_num_files(
        ".zap.clfd",
        ".bmwt.clfd",
        logfile=outfile_beamWeight,
        force_proceed=args.force_proceed,
    )

if skipnum < 5:
    outfile_scrunch = processing_scrunch(
        f"{processingjob_base} -J scrunch_{args.pulsar} ", args.tjob_scrunch, args.pulsar, newdata=True
    )
    check_num_files(
        ".bmwt.clfd", ".ftp", logfile=outfile_scrunch, force_proceed=args.force_proceed
    )

exp_timcreation = [
    "TOA generation using new data",
    "Adjust tjob with --tjob_tim",
]
cmd_timcreation = "./tim_creation.sh"
my_cmd(cmd_timcreation, exp_timcreation)

exp_newtim = "Creating our new tim file using pat."
outfile_newtim = f"new_tim_{args.pulsar}.out"
cmd_newtim = sbatch_cmd(
    "tim_run.sh",
    email,
    mem="66G",
    outfile=outfile_newtim,
    tjob=args.tjob_tim,
)
outfile_newtim = my_cmd(cmd_newtim, exp_newtim, checkcomplete=outfile_newtim)

timrun_sh = open("tim_run.sh", "r")
trr = timrun_sh.read()
timrun_sh.close()
newtimfile = [x.split(">")[-1].strip() for x in trr.split("\n") if len(x) > 0][-1]
if not os.path.exists(newtimfile):
    print(f"error: could not find tim file {newtimfile}!")
    exit(1)

# Rename TOAs with lowercase "chime_" so tempo2 doesn't think every line is a comment
newtim = open(newtimfile, "r")
tfr = newtim.read()
newtim.close()
lines = tfr.split("\n")
newlines = [
    f"chime{x.lstrip('CHIME')}\n" if x.startswith("CHIME") else ""
    for x in lines
]
tim_new = open(args.tim, "a")
for newline in newlines:
    tim_new.write(newline)
tim_new.close()