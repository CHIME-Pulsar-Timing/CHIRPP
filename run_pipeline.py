#!/usr/bin/env python

import argparse
import subprocess
import os
import numpy as np
import astropy.units as u
from glob import glob


def my_cmd(cmd, message, checkcomplete=False, renamelog=True):
    # Execute command and print explanation text
    if type(message) == list:  # Print multiple lines of explanation text
        for line in message:
            print(line)
    else:
        print(message)
    print(f"\n> {cmd}\n")  # Print command
    subprocess.run(cmd, shell=True)  # Execute command
    if checkcomplete:
        # Check the .out file for JOB CANCELLED messages
        # and rename it including the job ID, if necessary
        logfile = check_jobcomplete(checkcomplete, renamelog=True)
        # Check the .err file for JOB CANCELLED messages
        _ = check_jobcomplete(f"{logfile[:-4]}.err")
        return logfile


def check_jobcomplete(logfile, renamelog=False):
    lf = open(logfile, "r")
    lfr = lf.read()
    lf.close()
    logfile = logfile
    if renamelog:
        try:
            job_id = [x.split()[2] for x in lfr.split('\n') if x.startswith("Job ID:")][0]
            newlogfile = f"{logfile[:-4]}-{job_id}.out"
            cmd = f"mv {logfile} {newlogfile}"
            print(f"\n> {cmd}\n")
            subprocess.run(cmd, shell=True)
            logfile = newlogfile
        except IndexError:
            print(f"\nCouldn't rename {logfile}: found no line giving the SLURM job ID.\n")
    if "TIME LIMIT" in lfr:
        print(f"\nerror: job exceeded time limit. See log file: {logfile}\n")
        exit(1)
    elif "CANCELLED" in lfr:
        print(f"\nerror: job cancelled. See log file: {logfile}\n")
        exit(1)
    elif "OOM Killed" in lfr:
        print(f"\nerror: job ran out of memory. See log file: {logfile}\n")
        exit(1)
    else:
        return logfile


def edit_lines(fname, param_dict):
    # Edit a file given a dictionary of parameters to be changed
    # Lines to be edited need to have format:
    # '{parameter}={value}'   (whitespace is permitted)
    f_ext = fname.split(".")[-1]
    fname_new = f"{fname[:-len(f_ext)-1]}_new.{f_ext}"
    f = open(fname, "r")
    fr = f.read()
    f.close()
    fnew = open(fname_new, "w")  # Temporary version of file to be edited
    for line in fr.split("\n"):
        param = next(
            (s for s in param_dict.keys() if line.lstrip().startswith(s)), None
        )
        if f"{param}=" in line:
            param_value = param_dict[param]
            if not param_value:
                newline = line
            elif type(param_value) == str:
                newline = f'{line.split("=")[0]}="{param_value}"'
            else:
                newline = f'{line.split("=")[0]}={param_value}'
        else:
            newline = line
        fnew.write(f"{newline}\n")
    fnew.close()
    # Replace file with edited version
    a = subprocess.run(f"mv {fname_new} {fname}", shell=True)


def get_template_nbin(outfile_paramcheck):
    template_nbin = int(
        subprocess.run(
            f"cat {outfile_paramcheck} | grep template_nbin | awk -F= "
            + "'{print $2}' | awk '{print $1}'",
            shell=True,
            stdout=subprocess.PIPE,
        )
        .stdout.decode("utf-8")
        .strip("\n")
    )
    return template_nbin


def pathcheck(path):
    pathexists = os.path.exists(path)
    if not pathexists:
        print(f"\nerror: {path} is not a valid path!\n")
        exit(1)


def check_num_files(ext_1, ext_2, logfile=None, force_proceed=False):
    # Check that the number of files with each extension are the same
    N1 = len(glob(f"*{ext_1}"))
    N2 = len(glob(f"*{ext_2}"))
    if N1 == N2:
        print(f"\nSame number of files with extensions {ext_1} and {ext_2}\n")
    else:
        print(f"\nerror: {N1} files with extension {ext_1}, but {N2} with {ext_2}!\n")
        if logfile:
            print(
                f"Investigate the reason, e.g. by checking {logfile} or {logfile[:-4]}.err"
            )
            if force_proceed:
                print("Proceeding without the missing files.\n")
            else:
                proceed = False
                while not proceed:
                    proceed = input(
                        "Would you would like to proceed without the missing files? [y/n]\n"
                    )
                    if proceed == "y" or proceed == "Y":
                        return
                    elif proceed == "n" or proceed == "N":
                        exit(0)
                    else:
                        proceed = False
        else:
            exit(1)


def processing_scrunch(paralleljob_base, tjob_scrunch, pulsar):
    exp_scrunch = [
        "Scrunch in frequency, time, and polarization.",
        "Adjust tjob with --tjob_scrunch",
    ]
    outfile_scrunch = f"scrunch_{pulsar}.out"
    cmd_scrunch = f"{paralleljob_base}--time={tjob_scrunch} -o {outfile_scrunch} parallel_scrunch.sh"
    outfile_scrunch = my_cmd(cmd_scrunch, exp_scrunch, checkcomplete=outfile_scrunch)
    return outfile_scrunch


def make_template(tjob_template, email, Link, Deku, pulsar):
    exp_templatecreation = [
        "Run the next generator script to create the next set of files",
        f"This creates a list of the 50 highest-S/N files that have the specified number of bins.",
        "Then it creates template_run.sh, which will be submitted next.",
    ]
    jobname_templatecreation = f"template50_creation_{pulsar}"
    outfile_templatecreation = f"{jobname_templatecreation}.out"
    cmd_templatecreation = sbatch_cmd(
        "template50_creation.sh",
        email,
        Link,
        mem="66G",
        jobname=jobname_templatecreation,
        outfile=outfile_templatecreation,
        tjob=tjob_template,
        Deku=Deku,
    )

    outfile_templatecreation = my_cmd(
        cmd_templatecreation,
        exp_templatecreation,
        checkcomplete=outfile_templatecreation,
    )

    exp_templaterun = [
        "Create the template using autotoa.",
        "Adjust tjob with --tjob_template",
    ]
    outfile_templaterun = f"template_run_{pulsar}.out"
    cmd_templaterun = sbatch_cmd(
        "template_run.sh",
        email,
        Link,
        mem="126G",
        outfile=outfile_templaterun,
        tjob=tjob_template,
        Deku=Deku,
    )

    outfile_templaterun = my_cmd(
        cmd_templaterun, exp_templaterun, checkcomplete=outfile_templaterun
    )

    # Get template filename
    templaterun = open("template_run.sh", "r")
    trr = templaterun.read()
    templaterun.close()
    templatefile = [
        x.split()[-1].strip()
        for x in trr.split("\n")
        if x.startswith("mv added.trimmed.sm")
    ][0]
    if not os.path.exists(templatefile):
        print(f"error: could not find template file {templatefile}!")
        exit(1)

    print(
        "\nBefore we continue, view some diagnostic plots in another window as a sanity check.\n"
    )
    print(
        "Recommended: use this command to inspect some of the profiles used to generate the template:"
    )
    print("pav -N 3,2 -DFT -M template_50.txt")
    print("Also inspect the phase-aligned, scrunched data; and the smoothed template:")
    print(f"pav -D -r 0.5 added.trimmed {templatefile}\n")
    if force_proceed:
        print("Proceeding without asking for manual input.\n")
    else:
        _ = input(
            "If the template looks reasonable given the actual data, press Enter to continue...\n"
        )

    # Edit config.sh with template filename
    template_dict = dict([("template", templatefile)])
    edit_lines("config.sh", template_dict)

    return outfile_templaterun, templatefile


def make_tim(tjob_tim, email, Link, pulsar, Deku, ntry):
    exp_timcreation = f"TOA generation: try {ntry}"
    cmd_timcreation = "./tim_creation.sh"

    my_cmd(cmd_timcreation, exp_timcreation)

    exp_timrun = [
        "Create our .tim file using pat.",
        "Adjust tjob with --tjob_tim",
    ]
    outfile_timrun = f"tim_run_{pulsar}.out"
    cmd_timrun = sbatch_cmd(
        "tim_run.sh",
        email,
        Link,
        mem="66G",
        outfile=outfile_timrun,
        tjob=tjob_tim,
        Deku=Deku,
    )

    outfile_timrun = my_cmd(cmd_timrun, exp_timrun, checkcomplete=outfile_timrun)

    # Check that 75% of TOAs have S/N >= 8
    # If not, return the scrunch_factor that should make that happen
    # Use the .tim file from tim_run.sh
    timrun_sh = open("tim_run.sh", "r")
    trr = timrun_sh.read()
    timrun_sh.close()
    timfile = [x.split(">")[-1].strip() for x in trr.split("\n") if len(x) > 0][-1]
    if not os.path.exists(timfile):
        print(f"error: could not find tim file {timfile}!")
        exit(1)

    snr_25pct, snr_mean, _ = get_snr_pct(timfile=timfile)
    scrunch_factor = get_scrunch_factor(snr_25pct)

    scrunch_txt = open("scrunch.txt", "r")
    scr_line1 = [x for x in scrunch_txt.read().split("\n") if "--setnchn" in x][0]
    scrunch_txt.close()
    tim_nchan = int(scr_line1.split("--setnchn")[1].split()[0])

    # Rename TOAs with lowercase "chime_" so tempo2 doesn't think every line is a comment
    tim = open(timfile, "r")
    tfr = tim.read()
    tim.close()
    lines = tfr.split("\n")
    newlines = [
        f"chime{x.lstrip('CHIME')}\n" if x.startswith("CHIME") else f"{x}\n"
        for x in lines
    ]
    timfile_new = f"{timfile[:-4]}_new.tim"
    tim_new = open(timfile_new, "w")
    for newline in newlines:
        tim_new.write(newline)
    tim_new.close()
    # Replace file with edited version
    a = subprocess.run(f"mv {timfile_new} {timfile}", shell=True)

    return (
        timfile,
        outfile_timrun,
        tim_nchan,
        snr_25pct,
        snr_mean,
        scrunch_factor,
        len(newlines),
    )


def sbatch_cmd(
    script,
    email,
    Link,
    mem,
    jobname=None,
    outfile=None,
    tjob=None,
    misc=None,
    Deku=False,
):
    # Standardized format for our sbatch commands, allowing us to easily slot in optional flags
    sbatch_cmd = f"sbatch -W {email} "
    if not Link:
        if mem:
            sbatch_cmd += f"--mem={mem} "
        else:
            print("error: unspecified mem-per-cpu on Cedar!")
            exit(1)
        sbatch_cmd += f"--account=def-istairs "
    if Deku and Link:
        sbatch_cmd += f"--partition Deku "
    if jobname:
        sbatch_cmd += f"-J {jobname} "
    if outfile:
        sbatch_cmd += f"-o {outfile} "
    if tjob:
        sbatch_cmd += f"--time={tjob} "
    if misc:
        sbatch_cmd += f"{misc} "
    if script:
        sbatch_cmd += f"{script}"
    return sbatch_cmd


def maxtime(t1_str, t2_str):
    # Return the larger of two durations given in "HH:MM:SS" format.
    t1hms = t1_str.split(":")
    t2hms = t2_str.split(":")
    t1 = float(t1hms[0]) + float(t1hms[1]) / 60.0 + float(t1hms[2]) / 3600.0
    t2 = float(t2hms[0]) + float(t2hms[1]) / 60.0 + float(t2hms[2]) / 3600.0
    i = np.argmax(np.array([t1, t2]))
    return [t1_str, t2_str][i]


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
    description="Run TOA generation pipeline for CHIME observations of NANOGrav pulsars"
)
parser.add_argument("pulsar", type=str, help="Pulsar name, e.g. J0437-4715")
parser.add_argument(
    "-s",
    "--scripts_dir",
    type=str,
    help="Parent directory of chime-o-grav-scripts (Required if starting at the beginning!)",
)
parser.add_argument(
    "--link",
    action="store_true",
    help="Running on Link",
)
parser.add_argument(
    "-e",
    "--email",
    type=str,
    help="Email to be alerted when jobs complete or fail (nb: may not work on Link)",
)
parser.add_argument(
    "--skip", choices=pipeline_steps, type=str, help="Skip to the specified step"
)
parser.add_argument(
    "--paramcheck_cpus",
    type=int,
    default=10,
    help="Number of CPUs allParamCheck.sh can assign to GNU Parallel (10 by default)",
)
parser.add_argument(
    "--tjob_unpack",
    type=str,
    default="12:00:00",
    help="Time allotted to unpack_tar.sh job, HH:MM:SS (12h/24h by default on Cedar/Link)",
)
parser.add_argument(
    "--tjob_paramcheck",
    type=str,
    default="12:00:00",
    help="Time allotted to allParamCheck.sh job, HH:MM:SS (12h/24h by default on Cedar/Link)",
)
parser.add_argument(
    "--tjob_ephemNconvert",
    type=str,
    default="12:00:00",
    help="Time allotted to parallel_ephemNconvert.sh job, HH:MM:SS (12h/24h by default on Cedar/Link)",
)
parser.add_argument(
    "--tjob_clean5G",
    type=str,
    default="3:00:00",
    help="Time allotted to parallel_clean5G.sh job, HH:MM:SS (3h/24h by default on Cedar/Link)",
)
parser.add_argument(
    "--tjob_clean",
    type=str,
    default="3:00:00",
    help="Time allotted to parallel_clean.sh job, HH:MM:SS (3h/24h by default on Cedar/Link)",
)
parser.add_argument(
    "--tjob_beamweight",
    type=str,
    default="3:00:00",
    help="Time allotted to parallel_beamWeight.sh job, HH:MM:SS (3h/24h by default on Cedar/Link)",
)
parser.add_argument(
    "--tjob_scrunch",
    type=str,
    default="3:00:00",
    help="Time allotted to parallel_scrunch.sh job, HH:MM:SS (3h/24h by default on Cedar/Link)",
)
parser.add_argument(
    "--tjob_template",
    type=str,
    default="3:00:00",
    help="Time allotted to template_run.sh job, HH:MM:SS (3h/24h by default on Cedar/Link)",
)
parser.add_argument(
    "--tjob_tim",
    type=str,
    default="3:00:00",
    help="Time allotted to tim_run.sh job, HH:MM:SS (3h/24h by default on Cedar/Link)",
)
parser.add_argument(
    "-d",
    "--data_directory",
    type=str,
    default=current_dir,
    help="Path where the data live, or where you want the archives to be copied (current directory by default)",
)
group = parser.add_mutually_exclusive_group()
group.add_argument(
    "-p",
    "--par_directory",
    type=str,
    default="default_par_dir",
    help="Path to your par files. If left unspecified, default locations are checked on Cedar or Link.",
)
group.add_argument("--par", type=str, help="Provide a .par file to use directly.")
parser.add_argument(
    "--subint_threshold",
    type=float,
    default=None,
    help="allParamCheck.sh fails subints if duration not within 10 s +/- threshold (0.01 s by default)",
)
parser.add_argument(
    "--template_ext",
    type=str,
    default=None,
    help="Extension of files used to create template ('.ftp' by default)",
)
parser.add_argument(
    "--tim_flags",
    type=str,
    default=None,
    help="TOA flags in the format `python data_checks.py '-f=-f CHIME [...]'` (leave unspecified if you're not sure)",
)
parser.add_argument(
    "--min_nchan",
    type=int,
    default=8,
    help="Minimum number of subbands to scrunch to (8 by default)",
)
parser.add_argument(
    "--max_nchan",
    type=int,
    default=None,
    help="Maximum number of subbands to scrunch to (64 by default)",
)
parser.add_argument(
    "--no_Deku",
    action="store_true",
    help="On Link, do not use the Deku partition (GPUs) (not recommended)",
)
parser.add_argument(
    "-f",
    "--force_proceed",
    action="store_true",
    help="Automatically proceed through any prompts for manual input (not recommended, especially when running for the first time)",
)
args = parser.parse_args()

pulsar = args.pulsar
scripts_dir = args.scripts_dir
Link = args.link

if args.email and "@" not in args.email:
    email = f"--mail-user={args.email}@nanograv.org --mail-type=END,FAIL"
elif args.email:
    email = f"--mail-user={args.email} --mail-type=END,FAIL"
else:
    email = ""

try:
    skipnum = np.where(args.skip == pipeline_steps)[0][0]
except IndexError:
    skipnum = -1

paramcheck_cpus = args.paramcheck_cpus

# On Link, the time requests don't matter nearly as much. Request a minimum of 24h.
tjob_unpack = maxtime("24:00:00", args.tjob_unpack) if Link else args.tjob_unpack
tjob_paramcheck = (
    maxtime("24:00:00", args.tjob_paramcheck) if Link else args.tjob_paramcheck
)
tjob_ephemNconvert = (
    maxtime("24:00:00", args.tjob_ephemNconvert) if Link else args.tjob_ephemNconvert
)
tjob_clean5G = maxtime("24:00:00", args.tjob_clean5G) if Link else args.tjob_clean5G
tjob_clean = maxtime("24:00:00", args.tjob_clean) if Link else args.tjob_clean
tjob_beamweight = (
    maxtime("24:00:00", args.tjob_beamweight) if Link else args.tjob_beamweight
)
tjob_scrunch = maxtime("24:00:00", args.tjob_scrunch) if Link else args.tjob_scrunch
tjob_template = maxtime("24:00:00", args.tjob_template) if Link else args.tjob_template
tjob_tim = maxtime("24:00:00", args.tjob_tim) if Link else args.tjob_tim

data_dir = args.data_directory
par_dir = args.par_directory
parfile = args.par
threshold = args.subint_threshold
template_ext = args.template_ext
flags = args.tim_flags
min_nchan = args.min_nchan
max_nchan = args.max_nchan
Deku = not args.no_Deku
force_proceed = args.force_proceed

pathcheck(data_dir)

HOME = subprocess.check_output("echo $HOME", shell=True, text=True).strip("\n")

if parfile and os.path.exists(parfile):
    print(f"Par file found: {parfile}\n")
    if "/" in parfile:
        n = len(parfile.split("/")[-1])
        par_dir = parfile[:-n]
    else:
        par_dir = os.getcwd()
elif parfile and not os.path.exists(parfile):
    print(f"{parfile} not found!\n")
    exit(1)
else:
    # Check here first (on Cedar)
    DR3par_dir = f"{HOME}/projects/rrg-istairs-ad/DR3/NANOGrav_15y/par/tempo2"
    # Check here if no pars are found
    backuppar_dir = f"{HOME}/projects/rrg-istairs-ad/timing/tzpar"
    # Check here on Link
    Linkpar_dir = "/nanograv/timing/releases/20y/CHIME/pars"
    if par_dir != "default_par_dir":  # check directories for a valid par file
        pathcheck(par_dir)
        pars = glob(f"{par_dir}/*{pulsar}*.par")
        if len(pars) > 0:
            parfile = pars[0]
        else:
            print(f"\nerror: no .par file found for {pulsar} in {par_dir}!\n")
            exit(1)
    elif Link:
        pathcheck(Linkpar_dir)
        Linkpars = glob(f"{Linkpar_dir}/*{pulsar}*.par")
        if len(Linkpars) > 0:
            parfile = Linkpars[0]
            par_dir = Linkpar_dir
        else:
            print(f"\nerror: no .par file found for {pulsar} in {Linkpar_dir}!\n")
            exit(1)
    else:
        pathcheck(DR3par_dir)
        pathcheck(backuppar_dir)
        DR3pars = glob(f"{DR3par_dir}/*{pulsar}*.par")
        backuppars = glob(f"{backuppar_dir}/*{pulsar}*.par")
        if len(DR3pars) > 0:
            parfile = DR3pars[0]
            par_dir = DR3par_dir
        elif len(backuppars) > 0:
            parfile = backuppars[0]
            par_dir = backuppar_dir
        else:
            print(
                f"\nerror: no .par file found for {pulsar} in {DR3par_dir} or {backuppar_dir}!\n"
            )
            exit(1)
print(f'Using "{par_dir}" as par_directory.')
print(f"Found .par file in par_directory: {parfile.split('/')[-1]}\n")

if skipnum == -1:
    if not scripts_dir:
        print(
            "error: you need to provide the parent directory of the chime-o-grav-scripts repo with `-s` or `--scripts_dir` when starting at the beginning!\n"
        )
        exit(1)
    else:
        pathcheck(scripts_dir)
    exp_scripts = "Copy all scripts into the working directory."
    cmd_scripts = f"cp {scripts_dir}/chime-o-grav-scripts/mercedes_scripts/v3_pipeline/*sh {scripts_dir}/chime-o-grav-scripts/mercedes_scripts/v3_pipeline/*.py ."

    exp_exec = "Give scripts execution permissions."
    cmd_exec = "chmod +x *.sh"

    exp_newdata = "Grab the most recent data."
    data_cedar = f"{HOME}/projects/rrg-istairs-ad/archive/pulsar/fold_mode"
    data_link = f"/nanograv/archive/CHIME/NANOGrav/{pulsar}"
    if Link:
        pathcheck(data_link)
        cmd_newdata = f"ln -s {data_link}/*.ar {data_dir}"
    else:
        pathcheck(data_cedar)
        cmd_newdata = f"ln -s {data_cedar}/*{pulsar}*.ar {data_dir}"

    my_cmd(cmd_scripts, exp_scripts)
    my_cmd(cmd_exec, exp_exec)
    my_cmd(cmd_newdata, exp_newdata)

    if not Link:
        exp_olddata = "Grab the older data, this will take a minute."
        cmd_olddata = f"cp {HOME}/nearline/rrg-istairs-ad/archive/pulsar/chime/fold_mode/{pulsar}/*tar {data_dir}"
        my_cmd(cmd_olddata, exp_olddata)
        os.chdir(data_dir)
        exp_unpack = [
            f"Unpack old data to {data_dir}.",
            "Adjust tjob with --tjob_unpack",
        ]
        jobname_unpack = f"unpack_tar_{pulsar}"
        outfile_unpack = f"{jobname_unpack}.out"
        cmd_unpack = sbatch_cmd(
            "unpack_tar.sh",
            email,
            Link,
            mem="66G",
            jobname=jobname_unpack,
            outfile=outfile_unpack,
            tjob=tjob_unpack,
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
            if force_proceed:
                print("Proceeding without asking for manual input.\n")
            else:
                _ = input(
                    "Or, you can ctrl-C to investigate, then resume by running again with the `--skip checks` option.\n"
                )

from find_nchan import *

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
                "Are you sure the .par file you're using is sufficiently up-to-date? You may wish to grab one of the recent predictive .par files from:"
            )
            print(
                "    https://gitlab.nanograv.org/nano-time/timing_analysis/-/tree/15yr/release_tools/pred_par\n"
            )
            if not force_proceed:
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
    jobname_paramcheck = f"allParamCheck_{pulsar}"
    outfile_paramcheck = f"{jobname_paramcheck}.out"
    cmd_paramcheck = sbatch_cmd(
        "allParamCheck.sh",
        email,
        Link,
        mem="66G",
        jobname=jobname_paramcheck,
        outfile=outfile_paramcheck,
        tjob=tjob_paramcheck,
        misc=f"-c {paramcheck_cpus}",
        Deku=Deku,
    )
    my_cmd(cmd_datecheck, exp_datecheck)
    if threshold:
        print("\nEditing allParamCheck.sh with given threshold value\n")
        threshold_dict = dict([("threshold", threshold)])
        edit_lines("allParamCheck.sh", threshold_dict)
    outfile_paramcheck = my_cmd(
        cmd_paramcheck, exp_paramcheck, checkcomplete=outfile_paramcheck
    )
else:  # If not running allParamCheck.sh, use most recent output file
    try:
        outfile_paramcheck = sorted(glob(f"allParamCheck_{pulsar}*.out"))[-1]
    except IndexError:
        outfile_paramcheck = None

if outfile_paramcheck:
    template_nbin = get_template_nbin(outfile_paramcheck)
    print(f"\ntemplate_nbin from {outfile_paramcheck}: {template_nbin}\n")
else:
    print(f"\nNo allParamCheck_{pulsar}-[jobID].out found.\n")
    template_nbin = None
    if force_proceed:
        print("Proceeding with template_nbin value stored in config.sh.\n")
    else:
        while not template_nbin:
            print(
                "Press Enter to continue with the template_nbin currently in config.sh, or type in your desired value."
            )
            nbin_str = input(
                f"Or, you may want to ctrl-C then run allParamCheck: `python run_pipeline.py -s {scripts_dir} --skip checks {pulsar}`\n"
            )
            try:
                nbin = int(nbin_str)
                if nbin > 0 and (nbin & (nbin - 1)) == 0:
                    template_nbin = nbin
                else:
                    print("You must enter an integer power of two.\n")
            except ValueError:
                if nbin_str == "":
                    break
                else:
                    print("You must enter an integer power of two.\n")


print("Editing config.sh\n")
param_names = [
    "data_directory",
    "par_directory",
    "template_ext",
    "tim_flags",
    "template_nbin",
    "max_subint",
    "nsubbands",
]
param_values = [
    data_dir,
    par_dir,
    template_ext,
    flags,
    template_nbin,
    max_subint,
    max_nchan,
]
config_dict = dict(zip(param_names, param_values))
edit_lines("config.sh", config_dict)

if skipnum < 7:
    exp_processing_creation = (
        "Make the files that list the commands to run with GNU parallel."
    )
    cmd_processing_creation = "./processing_creation.sh"
    my_cmd(cmd_processing_creation, exp_processing_creation)

paralleljob_base = sbatch_cmd(None, email, Link, mem="126G", Deku=Deku)

if skipnum < 3:
    exp_ephemNconvert = [
        "Install ephemeris before averaging to ensure best data quality.",
        "Adjust tjob with --tjob_ephemNconvert",
    ]
    outfile_ephemNconvert = f"ephemNconvert_{pulsar}.out"
    cmd_ephemNconvert = f"{paralleljob_base}--time={tjob_ephemNconvert} -o {outfile_ephemNconvert} parallel_ephemNconvert.sh"
    outfile_ephemNconvert = my_cmd(
        cmd_ephemNconvert, exp_ephemNconvert, checkcomplete=outfile_ephemNconvert
    )

if skipnum < 4:
    exp_clean5G = [
        "Zap known bad channels on all archive files (*.ar).",
        "Adjust tjob with --tjob_clean5G",
    ]
    outfile_clean5G = f"clean5G_{pulsar}.out"
    cmd_clean5G = f"{paralleljob_base}--time={tjob_clean5G} -o {outfile_clean5G} parallel_clean5G.sh"
    outfile_clean5G = my_cmd(cmd_clean5G, exp_clean5G, checkcomplete=outfile_clean5G)
    check_num_files(".ar", ".zap", logfile=outfile_clean5G, force_proceed=force_proceed)

if skipnum < 5:
    exp_clean = [
        "Run clfd.",
        "Adjust tjob with --tjob_clean",
    ]
    outfile_clean = f"clean_{pulsar}.out"
    cmd_clean = (
        f"{paralleljob_base}--time={tjob_clean} -o {outfile_clean} parallel_clean.sh"
    )
    outfile_clean = my_cmd(cmd_clean, exp_clean, checkcomplete=outfile_clean)
    check_num_files(
        ".zap", ".zap.clfd", logfile=outfile_clean, force_proceed=force_proceed
    )

if skipnum < 6:
    exp_beamWeight = [
        "Run beam weighting.",
        "Adjust tjob with --tjob_beamweight",
    ]
    outfile_beamWeight = f"beamWeight_{pulsar}.out"
    cmd_beamWeight = f"{paralleljob_base}--time={tjob_beamweight} -o {outfile_beamWeight} parallel_beamWeight.sh"
    outfile_beamWeight = my_cmd(
        cmd_beamWeight, exp_beamWeight, checkcomplete=outfile_beamWeight
    )
    check_num_files(
        ".zap.clfd",
        ".bmwt.clfd",
        logfile=outfile_beamWeight,
        force_proceed=force_proceed,
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
    if force_proceed:
        print("Proceeding without asking for manual input.\n")
    else:
        _ = input(
            "If you don't notice any drifting in pulse phase in both sets of plots, press Enter to continue...\n"
        )

if skipnum < 7:
    outfile_scrunch = processing_scrunch(paralleljob_base, tjob_scrunch, pulsar)
    check_num_files(
        ".bmwt.clfd", ".ftp", logfile=outfile_scrunch, force_proceed=force_proceed
    )

if skipnum < 8:
    outfile_templaterun, templatefile = make_template(
        tjob_template, email, Link, Deku, pulsar
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
        make_tim(tjob_tim, email, Link, pulsar, Deku, ntry)
    )
    # Check if our S/N cut requires we scrunch further in frequency
    new_nchan = get_nchan(scrunch_factor, min_nchan=min_nchan, nchan_initial=tim_nchan)
    while new_nchan < tim_nchan:
        print(f"\nToo many low-S/N TOAs: 25th-percentile S/N is {snr_25pct:.2f}")
        print(
            f"Ideal scrunch factor: (8.0 / {snr_25pct:.2f})^2 raised to next power of two = {scrunch_factor}."
        )
        print(
            f"Scrunch by a factor of {scrunch_factor}, to a minimum of {min_nchan} subbands: trying again with {new_nchan} subbands.\n"
        )

        # Rename tim file from previous try
        mvtim = f"mv {timfile} {pulsar}.CHIME.try{ntry}_{tim_nchan}_subbands.tim"
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
        outfile_scrunch = processing_scrunch(paralleljob_base, tjob_scrunch, pulsar)
        (
            timfile,
            outfile_timrun,
            tim_nchan,
            snr_25pct,
            snr_mean,
            scrunch_factor,
            ntoas,
        ) = make_tim(tjob_tim, email, Link, pulsar, Deku, ntry)
    print(f"\n25th-percentile S/N: {snr_25pct:.2f}")
    print(f"Mean S/N: {snr_mean:.2f}")
    print(
        f"Ideal scrunch factor: (8.0 / {snr_25pct})^2 raised to next power of two = {scrunch_factor}.\n"
    )
    if new_nchan == min_nchan:
        print(f"We have reached the minimum number of subbands ({min_nchan}).\n")
    else:
        print(
            f"The current number of subbands ({tim_nchan}) satisfies our S/N cut condition.\n"
        )
else:
    try:
        templatefile = sorted(glob(f"{pulsar}.Rcvr_CHIME.CHIME.????-??-??.sum.sm"))[-1]
    except IndexError:
        templatefile = "mytemplate.sm"
    try:
        timfile = sorted(glob(f"{pulsar}.Rcvr_CHIME.CHIME.????-??-??.nb.tim"))[-1]
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
if Link:
    print(
        f"To print the information below again, run `python run_pipeline.py --link --skip complete {pulsar}`"
    )
else:
    print(
        f"To print the information below again, run `python run_pipeline.py --skip complete {pulsar}`"
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
print(
    f'Otherwise, copy the .tim file to the central directory on {"Link" if Link else "Cedar"}:\n'
)
if Link:
    NG20data = "/nanograv/timing/releases/20y/CHIME/toas/"
else:
    NG20data = f"{HOME}/projects/rrg-istairs-ad/CHIMEnNG20_TOAs/"
print(f"cp {timfile} {NG20data}\n")
print(
    f"Next, submit a merge request to add {timfile} to the NG20 repo, if you have access:"
)
print("   https://gitlab.nanograv.org/nano-time/ng20/-/tree/main/toas\n")
# Check if pulsar is on list of IPTA DR3 pulsars.
DR3_list = [
    "B1937+21",
    "J0030+0451",
    "J0613-0200",
    "J1012+5307",
    "J1640+2224",
    "J1643-1224",
    "J1713+0747",
    "J1744-1134",
    "J1918-0642",
    "J2145-0750",
    "J2317+1439",
]
if pulsar in DR3_list:
    print(
        "It looks like your pulsar is also an IPTA DR3 pulsar. Submit an MR to that repo as well (create a new directory under CHIME, if necessary):"
    )
    print("   https://gitlab.com/IPTA/DR3/-/tree/CHIME_TOAs/CHIME\n")
print(
    "If you don't have access to the gitlab repository, let folks know in the #chime-o-grav Slack channel that an MR should be submitted.\n"
)
print(
    "Finally, zip the scripts you used, Slurm output/error files, RFI-zapped files (pre- and post-scrunching), and template to the same directory:\n"
)
tarball = f"{pulsar}.CHIME.20y.data.tar.gz"
print(
    f"tar -czf {tarball} *sh *.py *.out *.err CHIME*{pulsar}*.bmwt.clfd CHIME*{pulsar}*.bmwt.ftp added.trimmed {templatefile}"
)
print(f"mv {tarball} {NG20data}\n")
print(
    "Once you complete these steps, you have completed the TOA gen pipeline for this pulsar! Hooray!\n"
)
