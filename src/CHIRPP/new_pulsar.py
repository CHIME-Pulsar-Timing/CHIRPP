#!/usr/bin/env python

import argparse
import subprocess
import os
import numpy as np
import astropy.units as u
from glob import glob
from CHIRPP_utils import *


current_dir = subprocess.check_output("pwd", shell=True, text=True).strip("\n")
pipeline_steps = np.array(
    [
        "checks",
        "processing",
        "ephemNconvert",
        "clean5G",
        "clean",
        "beamWeight",
        "scrunch",
        "template",
        "tim",
        "complete",
    ]
)

parser = argparse.ArgumentParser(
    description="Generate template profile and times of arrival for new pulsar dataset."
)
parser.add_argument("pulsar", type=str, help="Pulsar name, e.g. J0437-4715")
parser.add_argument(
    "-s",
    "--scripts_dir",
    type=str,
    help="Path to CHIRPP repository (e.g. '~/CHIRPP'; required if starting at the beginning)",
)
parser.add_argument(
    "-e",
    "--email",
    type=str,
    help="Email to be alerted when jobs complete or fail.",
)
parser.add_argument(
    "--skip", choices=pipeline_steps, type=str, help="Skip to the specified step."
)
parser.add_argument(
    "--max_cpus",
    type=int,
    default=10,
    help="Maximum number of CPUs to assign allParamCheck.sh job (10 by default).",
)
parser.add_argument(
    "--tjob_unpack",
    type=str,
    default="12:00:00",
    help="Time allotted to unpack_tar.sh job, HH:MM:SS (12h by default).",
)
parser.add_argument(
    "--tjob_paramcheck",
    type=str,
    default="12:00:00",
    help="Time allotted to allParamCheck.sh job, HH:MM:SS (12h by default).",
)
parser.add_argument(
    "--tjob_ephemNconvert",
    type=str,
    default="12:00:00",
    help="Time allotted to parallel_ephemNconvert.sh job, HH:MM:SS (12h by default).",
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
parser.add_argument(
    "-d",
    "--data_directory",
    type=str,
    default=current_dir,
    help="Path to copy the archives to (or where they already live if using --skip; current directory by default).",
)
group = parser.add_mutually_exclusive_group()
group.add_argument(
    "-p",
    "--par_directory",
    type=str,
    default="default_par_dir",
    help="Path to your par files. If left unspecified, default locations are checked.",
)
group.add_argument("--par", type=str, help="Provide a .par file to use directly.")
parser.add_argument(
    "--subint_threshold",
    type=float,
    default=None,
    help="allParamCheck.sh fails subints if duration not within 10 s +/- threshold (0.01 s by default).",
)
parser.add_argument(
    "--template_ext",
    type=str,
    default=None,
    help="Extension of files used to create template ('.ftp' by default).",
)
parser.add_argument(
    "--tim_flags",
    type=str,
    default=None,
    help="TOA flags in the format '-f=-f CHIME [...]' (including the quotes).",
)
parser.add_argument(
    "--par_dm",
    action="store_true",
    help="Update the file header DMs to the value in the provided par file.",
)
parser.add_argument(
    "--min_nchan",
    type=int,
    default=8,
    help="Minimum number of subbands to scrunch to (8 by default).",
)
parser.add_argument(
    "--max_nchan",
    type=int,
    default=None,
    help="Maximum number of subbands to scrunch to (64 by default).",
)
parser.add_argument(
    "-f",
    "--force_proceed",
    action="store_true",
    help="Automatically proceed through any prompts for manual input.",
)
args = parser.parse_args()

email = parse_email(args.email)

try:
    skipnum = np.where(args.skip == pipeline_steps)[0][0]
except IndexError:
    print(f"error: use a valid pipeline step with --skip, one of: {pipeline_steps}")
    exit(1)

pathcheck(args.data_directory)

HOME = subprocess.check_output("echo $HOME", shell=True, text=True).strip("\n")

if args.par and os.path.exists(args.par):
    print(f"Par file found: {args.par}\n")
    if "/" in args.par:
        n = len(args.par.split("/")[-1])
        par_dir = args.par[:-n]
    else:
        par_dir = os.getcwd()
elif args.par and not os.path.exists(args.par):
    print(f"{args.par} not found!\n")
    exit(1)
else:
    # Check here first
    DR3par_dir = f"{HOME}/projects/rrg-istairs-ad/DR3/NANOGrav_15y/par/tempo2"
    # Check here if no pars are found
    backuppar_dir = f"{HOME}/projects/rrg-istairs-ad/timing/tzpar"
    if (
        args.par_directory != "default_par_dir"
    ):  # check directories for a valid par file
        pathcheck(args.par_directory)
        pars = glob(f"{args.par_directory}/*{args.pulsar}*.par")
        if len(pars) > 0:
            parfile = pars[0]
            par_dir = args.par_directory
        else:
            print(
                f"\nerror: no .par file found for {args.pulsar} in {args.par_directory}!\n"
            )
            exit(1)
    else:
        pathcheck(DR3par_dir)
        pathcheck(backuppar_dir)
        DR3pars = glob(f"{DR3par_dir}/*{args.pulsar}*.par")
        backuppars = glob(f"{backuppar_dir}/*{args.pulsar}*.par")
        if len(DR3pars) > 0:
            parfile = DR3pars[0]
            par_dir = DR3par_dir
        elif len(backuppars) > 0:
            parfile = backuppars[0]
            par_dir = backuppar_dir
        else:
            print(
                f"\nerror: no .par file found for {args.pulsar} in {DR3par_dir} or {backuppar_dir}!\n"
            )
            exit(1)
print(f'Using "{par_dir}" as par_directory.')
print(f"Found .par file in par_directory: {parfile.split('/')[-1]}\n")

if skipnum == -1:
    if not args.scripts_dir:
        print(
            "error: you need to provide the path to the CHIRPP repository (e.g. '~/CHIRPP') with `-s` or `--scripts_dir` when starting at the beginning!\n"
        )
        exit(1)
    else:
        pathcheck(args.scripts_dir)
    exp_scripts = "Copy all scripts into the working directory."
    cmd_scripts = f"cp {args.scripts_dir}/src/CHIRPP/bash/*sh {args.scripts_dir}/*.py ."

    exp_exec = "Give scripts execution permissions."
    cmd_exec = "chmod +x *.sh"

    exp_newdata = "Grab the most recent data."
    foldmode_dir = f"{HOME}/projects/rrg-istairs-ad/archive/pulsar/fold_mode"
    pathcheck(foldmode_dir)
    cmd_newdata = f"ln -s {foldmode_dir}/*{args.pulsar}*.ar {args.data_directory}"

    my_cmd(cmd_scripts, exp_scripts)
    my_cmd(cmd_exec, exp_exec)
    my_cmd(cmd_newdata, exp_newdata)

    exp_olddata = "Grab the older data, this will take a minute."
    cmd_olddata = f"cp {HOME}/nearline/rrg-istairs-ad/archive/pulsar/chime/fold_mode/{args.pulsar}/*tar {args.data_directory}"
    my_cmd(cmd_olddata, exp_olddata)
    os.chdir(args.data_directory)
    exp_unpack = [
        f"Unpack old data to {args.data_directory}.",
        "Adjust tjob with --tjob_unpack",
    ]
    jobname_unpack = f"unpack_tar_{args.pulsar}"
    outfile_unpack = f"{jobname_unpack}.out"
    cmd_unpack = sbatch_cmd(
        "unpack_tar.sh",
        email,
        mem="66G",
        jobname=jobname_unpack,
        outfile=outfile_unpack,
        tjob=args.tjob_unpack,
    )
    outfile_unpack = my_cmd(cmd_unpack, exp_unpack, checkcomplete=outfile_unpack)

    # Check that all tar's were unpacked
    n_tars = len(glob("*.tar"))
    n_unpacked = (
        int(
            subprocess.run(
                f"cat {outfile_unpack} | grep tar | wc -l",
                shell=True,
                stdout=subprocess.PIPE,
            )
            .stdout.decode("utf-8")
            .strip("\n")
        )
        // 2
    )
    os.chdir(current_dir)
    if n_tars != n_unpacked:
        print(
            f"\nwarning: number of .tar files does not match number that were unpacked, according to {outfile_unpack}.\n"
        )
        print(
            "Take a look at the log to find out what happened. If all is well, you can resume by pressing Enter."
        )
        if args.force_proceed:
            print("Proceeding without asking for manual input.\n")
        else:
            _ = input(
                "Or, you can ctrl-C to investigate, then resume by running again with the `--skip checks` option.\n"
            )

## Get orbital period, if any, from par file. Needed later to determine T-scrunching factor ##
pf = open(parfile, "r")
pfr = pf.read()
pf.close()
# Get max_subint default value from config.sh
cf = open("config.sh", "r")
cfr = cf.read()
cf.close()
max_subint_default = [
    float(x.split("=")[1]) for x in cfr.split("\n") if x.split("=")[0] == "max_subint"
][0]
if "BINARY" in pfr:
    pb = -1
    for line in pfr.split("\n"):
        if line.startswith("PB "):
            pb = (
                float(line.split()[1].replace("D", "E")) * u.d
            )  # Orbital period from par file (in days)
        elif line.startswith("FB0"):
            # Orbital frequency from par file (in Hz)
            fb = float(line.split()[1].replace("D", "E")) * u.Hz
            pb = 1 / fb  # Orbital period = 1 / frequency
    if pb > 0:
        pb_s = pb.to(u.s).value  # Orbital period in seconds
        # Maximum subint duration in seconds = 2.5% of orbital period, if shorter than default
        max_subint = 0.025 * pb_s
        if max_subint > max_subint_default:
            max_subint = None  # Use default value in config.sh
        else:
            print(f"\nSetting max_subint to 2.5% of orbital period = {max_subint} s.\n")
        if pb < 2.0 * u.d:
            print(
                f"Your pulsar has an orbital period of {pb.to(u.h).value:.2f} hours. Orbital variations may cause the pulse to drift over time."
            )
            print(
                "Are you sure the .par file you're using is sufficiently up-to-date? If timing a NANOGrav 15-yr pulsar, you may wish to grab one of the recent predictive .par files from:"
            )
            print(
                "    https://gitlab.nanograv.org/nano-time/timing_analysis/-/tree/15yr/release_tools/pred_par\n"
            )
            if not args.force_proceed:
                proceed = False
                while not proceed:
                    proceed = input(
                        "Would you like to proceed with your current .par file? [y/n]\n"
                    )
                    if proceed == "y" or proceed == "Y":
                        pass
                    elif proceed == "n" or proceed == "N":
                        exit(0)
                    else:
                        proceed = False
            print("Proceeding with current .par file.\n")
    else:
        print(
            f"\nerror: orbital frequency/period not found in {parfile}! I checked for 'PB' and 'FB0'.\n"
        )
        exit(1)
else:
    max_subint = None  # Use default value in config.sh
    print(
        "\nNo orbital period/frequency found in .par file. Using default max_subint value.\n"
    )

if skipnum < 1:
    exp_datecheck = "Cut any files with MJD <= 58600."
    cmd_datecheck = "./dateCheck.sh"

    exp_paramcheck = [
        "The next job standardizes all parameter values (nbin,subint,nchan,freq,bw,npol,dm)",
        "Files that fail checks logged in ${PARAMETER}Fail/${PARAMETER}Fail.log",
        "Adjust tjob with --tjob_paramcheck.",
    ]
    jobname_paramcheck = f"allParamCheck_{args.pulsar}"
    outfile_paramcheck = f"{jobname_paramcheck}.out"
    cmd_paramcheck = sbatch_cmd(
        "allParamCheck.sh",
        email,
        mem="66G",
        jobname=jobname_paramcheck,
        outfile=outfile_paramcheck,
        tjob=args.tjob_paramcheck,
        misc=f"-c {args.paramcheck_cpus}",
    )
    my_cmd(cmd_datecheck, exp_datecheck)
    if args.subint_threshold:
        print("\nEditing allParamCheck.sh with given threshold value\n")
        threshold_dict = dict([("threshold", args.subint_threshold)])
        edit_lines("allParamCheck.sh", threshold_dict)
    outfile_paramcheck = my_cmd(
        cmd_paramcheck, exp_paramcheck, checkcomplete=outfile_paramcheck
    )
else:  # If not running allParamCheck.sh, use most recent output file
    try:
        outfile_paramcheck = sorted(glob(f"allParamCheck_{args.pulsar}*.out"))[-1]
    except IndexError:
        outfile_paramcheck = None

if outfile_paramcheck:
    template_nbin, dm = get_nbin_dm(outfile_paramcheck)
    print(f"\ntemplate_nbin from {outfile_paramcheck}: {template_nbin}\n")
    if args.par_dm:
        print("Using DM from par file.")
        dm = "ephemeris"
    else:
        print(f"\nDM from {outfile_paramcheck}: {dm}\n")
else:
    print(f"\nNo allParamCheck_{args.pulsar}-[jobID].out found.\n")
    template_nbin = None
    if args.force_proceed:
        print(
            "Proceeding with template_nbin value stored in config.sh, will not update DMs.\n"
        )
    else:
        print(
            "You can choose to proceed with the default values for template_nbin and DM or enter your own."
        )
        print(
            f"Or, you may want to ctrl-C then run from allParamCheck: `new_pulsar.py --skip checks {args.pulsar}`\n"
        )
        while not template_nbin:
            nbin_str = input(
                "Press Enter to continue with the template_nbin currently in config.sh, or type in your desired value.\n"
            )
            try:
                nbin = int(nbin_str.strip())
                if nbin > 0 and (nbin & (nbin - 1)) == 0:
                    template_nbin = nbin
                else:
                    print("You must enter an integer power of two.\n")
            except ValueError:
                if nbin_str == "":
                    break
                else:
                    print("You must enter an integer power of two.\n")
        if args.par_dm:
            print("Using DM from par file.")
            dm = "ephemeris"
        else:
            dm = None
            while not dm:
                print(
                    "Press Enter to continue without updating header DMs, or type in your desired value (in pc/cc)."
                )
                dm_str = input(
                    "You can also re-run with '--par_dm' to use the DM value in your par file.\n"
                )
                try:
                    dm = float(dm_str.strip())
                    if dm < 0.0:
                        dm = None
                        print("You must enter a non-negative number (in pc/cc).\n")
                except ValueError:
                    if dm_str == "":
                        break
                    else:
                        print("You must enter a non-negative number (in pc/cc).\n")


print("Editing config.sh\n")
param_names = [
    "data_directory",
    "par_directory",
    "template_ext",
    "tim_flags",
    "template_nbin",
    "dm",
    "max_subint",
    "nsubbands",
]
param_values = [
    args.data_directory,
    par_dir,
    args.template_ext,
    args.tim_flags,
    template_nbin,
    dm,
    max_subint,
    args.max_nchan,
]
config_dict = dict(zip(param_names, param_values))
edit_lines("config.sh", config_dict)

if skipnum < 7:
    exp_processing_creation = (
        "Make the files that list the commands to run with GNU parallel."
    )
    cmd_processing_creation = "./processing_creation.sh"
    my_cmd(cmd_processing_creation, exp_processing_creation)

paralleljob_base = sbatch_cmd(None, email, mem="126G")

if skipnum < 3:
    exp_ephemNconvert = [
        "Install ephemeris before averaging to ensure best data quality.",
        "Adjust tjob with --tjob_ephemNconvert",
    ]
    outfile_ephemNconvert = f"ephemNconvert_{args.pulsar}.out"
    cmd_ephemNconvert = f"{paralleljob_base}--time={args.tjob_ephemNconvert} -o {outfile_ephemNconvert} parallel_ephemNconvert.sh"
    outfile_ephemNconvert = my_cmd(
        cmd_ephemNconvert, exp_ephemNconvert, checkcomplete=outfile_ephemNconvert
    )

if skipnum < 4:
    exp_clean5G = [
        "Zap known bad channels on all archive files (*.ar).",
        "Adjust tjob with --tjob_clean5G",
    ]
    outfile_clean5G = f"clean5G_{args.pulsar}.out"
    cmd_clean5G = f"{paralleljob_base}--time={args.tjob_clean5G} -o {outfile_clean5G} parallel_clean5G.sh"
    outfile_clean5G = my_cmd(cmd_clean5G, exp_clean5G, checkcomplete=outfile_clean5G)
    check_num_files(
        ".ar", ".zap", logfile=outfile_clean5G, force_proceed=args.force_proceed
    )

if skipnum < 5:
    exp_clean = [
        "Run clfd.",
        "Adjust tjob with --tjob_clean",
    ]
    outfile_clean = f"clean_{args.pulsar}.out"
    cmd_clean = f"{paralleljob_base}--time={args.tjob_clean} -o {outfile_clean} parallel_clean.sh"
    outfile_clean = my_cmd(cmd_clean, exp_clean, checkcomplete=outfile_clean)
    check_num_files(
        ".zap", ".zap.clfd", logfile=outfile_clean, force_proceed=args.force_proceed
    )

if skipnum < 6:
    exp_beamWeight = [
        "Run beam weighting.",
        "Adjust tjob with --tjob_beamweight",
    ]
    outfile_beamWeight = f"beamWeight_{args.pulsar}.out"
    cmd_beamWeight = f"{paralleljob_base}--time={args.tjob_beamweight} -o {outfile_beamWeight} parallel_beamWeight.sh"
    outfile_beamWeight = my_cmd(
        cmd_beamWeight, exp_beamWeight, checkcomplete=outfile_beamWeight
    )
    check_num_files(
        ".zap.clfd",
        ".bmwt.clfd",
        logfile=outfile_beamWeight,
        force_proceed=args.force_proceed,
    )
    print(
        "\nBefore we scrunch the data, view some diagnostic plots in another window as a sanity check.\n"
    )
    print("Recommended: use these commands to inspect sets of six random plots:\n")
    print('pav -N 3,2 -dGTp $(find . -name "*.bmwt.clfd" | shuf | head -n 6)')
    print(
        "-dGTp:  Time/polarization-scrunched, dedispersed, frequency vs. phase plot.\n"
    )
    print('pav -N 3,2 -dYFp $(find . -name "*.bmwt.clfd" | shuf | head -n 6)')
    print(
        "-dYFp:  Frequency/polarization-scrunched, dedispersed, integration time vs. phase plot."
    )
    print(
        "To collect more sets of six plots to view in sequence, use head -n 12 or higher."
    )
    print(
        "If the pulsar signal is difficult to see, try scrunching by a few times in time (e.g. -t 4), frequency (-f 4), or phase bins (-b 4).\n"
    )
    if args.force_proceed:
        print("Proceeding without asking for manual input.\n")
    else:
        _ = input(
            "If you don't notice any drifting in pulse phase in both sets of plots, press Enter to continue...\n"
        )

if skipnum < 7:
    outfile_scrunch = processing_scrunch(
        paralleljob_base, args.tjob_scrunch, args.pulsar
    )
    check_num_files(
        ".bmwt.clfd", ".ftp", logfile=outfile_scrunch, force_proceed=args.force_proceed
    )

if skipnum < 8:
    outfile_templaterun, templatefile = make_template(
        args.tjob_template, email, args.pulsar
    )
else:
    # Get the template filename from config.sh if skipping template creation
    cf = open("config.sh", "r")
    cfr = cf.read()
    cf.close()
    try:
        config_template = [
            x.split("=")[1].strip('"').strip("'")
            for x in cfr.split("\n")
            if x.startswith("template=")
        ][0]
        templatefile = glob(config_template)[0]
    except IndexError:
        print(
            f"error: no template file found matching the filename in config.sh: {config_template}!"
        )
        print(
            "You'll have to start at a previous step, or change config.sh's 'template' parameter.\n"
        )
        exit(1)

if skipnum < 9:
    ntry = 1
    timfile, outfile_timrun, tim_nchan, snr_25pct, snr_mean, scrunch_factor, ntoas = (
        make_tim(args.tjob_tim, email, args.pulsar, ntry)
    )
    # Check if our S/N cut requires we scrunch further in frequency
    new_nchan = get_nchan(
        scrunch_factor, min_nchan=args.min_nchan, nchan_initial=tim_nchan
    )
    while new_nchan < tim_nchan:
        print(f"\nToo many low-S/N TOAs: 25th-percentile S/N is {snr_25pct:.2f}")
        print(
            f"Ideal scrunch factor: (8.0 / {snr_25pct:.2f})^2 raised to next power of two = {scrunch_factor}."
        )
        print(
            f"Scrunch by a factor of {scrunch_factor}, to a minimum of {args.min_nchan} subbands: trying again with {new_nchan} subbands.\n"
        )

        # Rename tim file from previous try
        mvtim = f"mv {timfile} {args.pulsar}.CHIME.try{ntry}_{tim_nchan}_subbands.tim"
        print(f"> {mvtim}\n")
        subprocess.run(mvtim, shell=True)

        ntry += 1

        scrunch_dict = dict([("nsubbands", new_nchan)])
        edit_lines("config.sh", scrunch_dict)

        # Edit scrunch.txt to use new nchan
        fname = "scrunch.txt"
        f = open(fname, "r")
        fr = f.read()
        f.close()
        fname_new = f"scrunch_new.txt"
        fnew = open(fname_new, "w")  # Temporary version of file to be edited
        for line in [x for x in fr.split("\n") if len(x) > 0]:
            left = line.split("--setnchn")[0]
            right = line.split("-e")[1]
            newline = f"{left}--setnchn {new_nchan} -e{right}\n"
            fnew.write(newline)
        fnew.close()
        # Replace file with edited version
        a = subprocess.run(f"mv {fname_new} {fname}", shell=True)

        print("config.sh and scrunch.txt edited to reflect new # of subbands.\n")

        # Run scrunch again, scrunching further in frequency this time.
        outfile_scrunch = processing_scrunch(
            paralleljob_base, args.tjob_scrunch, args.pulsar
        )
        (
            timfile,
            outfile_timrun,
            tim_nchan,
            snr_25pct,
            snr_mean,
            scrunch_factor,
            ntoas,
        ) = make_tim(args.tjob_tim, email, args.pulsar, ntry)
    print(f"\n25th-percentile S/N: {snr_25pct:.2f}")
    print(f"Mean S/N: {snr_mean:.2f}")
    print(
        f"Ideal scrunch factor: (8.0 / {snr_25pct})^2 raised to next power of two = {scrunch_factor}.\n"
    )
    if new_nchan == args.min_nchan:
        print(f"We have reached the minimum number of subbands ({args.min_nchan}).\n")
    else:
        print(
            f"The current number of subbands ({tim_nchan}) satisfies our S/N cut condition.\n"
        )
else:
    try:
        templatefile = sorted(
            glob(f"{args.pulsar}.Rcvr_CHIME.CHIME.????-??-??.sum.sm")
        )[-1]
    except IndexError:
        templatefile = "mytemplate.sm"
    try:
        timfile = sorted(glob(f"{args.pulsar}.Rcvr_CHIME.CHIME.????-??-??.nb.tim"))[-1]
        tf = open(timfile, "r")
        tfr = tf.read()
        tf.close()
        ntoas = len(tfr.split("\n"))
    except IndexError:
        timfile = "mytoas.tim"
        ntoas = None


print(
    "\n####################################################################################################\n"
)
print(
    f"To print the information below again, run `new_pulsar.py --skip complete {args.pulsar}`"
)
print(
    "\n####################################################################################################\n\n"
)
print("Pipeline completed! Take a look at your new .tim file, perhaps with...\n")
if ntoas:
    print(f"tempo2 -nofit -npsr 1 -nobs {ntoas+10} -gr plk -f {parfile} {timfile}\n")
else:
    print(f"tempo2 -nofit -npsr 1 -gr plk -f {parfile} {timfile}\n")

print("...or with PINT, as you prefer.")
print(
    f"If the timing solution is a poor fit to the data, consider re-running the pipeline with a re-fit .par file (use the --par option), starting from the processing step (--skip processing).\n"
)
