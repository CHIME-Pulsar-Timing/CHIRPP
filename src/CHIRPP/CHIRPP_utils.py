#! /usr/bin/env python

import subprocess
import os
import numpy as np
from glob import glob
from write_scripts import *


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
        logfile = check_jobcomplete(checkcomplete, renamelog=renamelog)
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
            job_id = [x.split()[2] for x in lfr.split("\n") if x.startswith("Job ID:")][
                0
            ]
            newlogfile = f"{logfile[:-4]}-{job_id}.out"
            cmd = f"mv {logfile} {newlogfile}"
            print(f"\n> {cmd}\n")
            subprocess.run(cmd, shell=True)
            logfile = newlogfile
        except IndexError:
            print(
                f"\nCouldn't rename {logfile}: found no line giving the SLURM job ID.\n"
            )
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
    subprocess.run(f"mv {fname_new} {fname}", shell=True)


def get_nbin_dm(outfile_paramcheck):
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
    dm = float(
        subprocess.run(
            f"cat {outfile_paramcheck} | grep common_dm | awk -F= "
            + "'{print $2}' | awk '{print $1}'",
            shell=True,
            stdout=subprocess.PIPE,
        )
        .stdout.decode("utf-8")
        .strip("\n")
    )
    return template_nbin, dm


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


def processing_scrunch(base_jobname, tjob_scrunch, pulsar, newdata=False):
    exp_scrunch = [
        "Scrunch in frequency, time, and polarization.",
        "Adjust tjob with --tjob_scrunch",
    ]
    outfile_scrunch = f"scrunch_{pulsar}.out"
    if newdata:
        cmd_scrunch = (
            f"{base_jobname}--time={tjob_scrunch} -o {outfile_scrunch} scrunch.sh"
        )
    else:
        cmd_scrunch = f"{base_jobname}--time={tjob_scrunch} -o {outfile_scrunch} parallel_scrunch.sh"
    outfile_scrunch = my_cmd(cmd_scrunch, exp_scrunch, checkcomplete=outfile_scrunch)
    return outfile_scrunch


def make_template(
    tjob_template, email, pulsar, force_proceed=False, force_overwrite=False
):
    exp_templatecreation = [
        "Run the next generator script to create the next set of files",
        f"This creates a list of the 50 highest-S/N files that have the specified number of bins.",
        "Then it creates template_run.sh, which will be submitted next.",
    ]
    jobname_templatecreation = f"template_creation_{pulsar}"
    outfile_templatecreation = f"{jobname_templatecreation}.out"
    cmd_templatecreation = sbatch_cmd(
        "template_creation.sh",
        email,
        mem="66G",
        jobname=jobname_templatecreation,
        outfile=outfile_templatecreation,
        tjob=tjob_template,
    )

    write_template_creation(force_overwrite=force_overwrite)
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
        mem="126G",
        outfile=outfile_templaterun,
        tjob=tjob_template,
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


def make_tim(tjob_tim, email, pulsar, ntry, force_overwrite=False, timtype=""):
    exp_timcreation = f"TOA generation: try {ntry}"
    cmd_timcreation = "./tim_creation.sh"

    write_tim_creation(force_overwrite=force_overwrite, timtype=timtype)
    my_cmd(cmd_timcreation, exp_timcreation)

    exp_timrun = [
        "Create our .tim file using pat.",
        "Adjust tjob with --tjob_tim",
    ]
    outfile_timrun = f"tim_run_{pulsar}.out"
    cmd_timrun = sbatch_cmd(
        "tim_run.sh",
        email,
        mem="66G",
        outfile=outfile_timrun,
        tjob=tjob_tim,
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
    subprocess.run(f"mv {timfile_new} {timfile}", shell=True)

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
    mem,
    jobname=None,
    outfile=None,
    tjob=None,
    misc=None,
):
    # Standardized format for our sbatch commands, allowing us to easily slot in optional flags
    sbatch_cmd = f"sbatch -W --account=def-istairs --mem={mem} {email} "
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


def get_scrunch_factor(snr_25pct, snr_threshold=8, mean_nsubint=1):
    """
    Determine the power of two to frequency scrunch by,
    given a S/N threshold and the 25th percentile S/N.
    """

    scrunch_factor = ((snr_threshold / snr_25pct) ** 2) // mean_nsubint
    if scrunch_factor < 1:
        # Do not scrunch further
        return 1
    power = 1
    # Always default to a higher power (fewer subbands).
    # Recall, fewer subbands means more signal in each channel,
    # ensuring no more than 25% being cut.
    while power <= scrunch_factor:
        # Increment by 2 e.g. 2, 4, 8...
        power *= 2
    return power


def get_snr_pct(percentile=25, extension=".bmwt.zap", max_subint=3600.0, timfile=None):
    """
    Determine the S/N value that excludes {percentile}% of the distribution,
    given either a .tim file or a collection of profiles.
    Also return the mean S/N and the mean number of subints per file (if applicable)
    """
    if timfile:
        tim = open(timfile, "r")
        tfr = tim.read()
        tim.close()
        snrs = np.array(
            [
                float(line.split("-snr")[1].split()[0])
                for line in tfr.split("\n")
                if "-snr" in line
                and (not line.startswith("C") or line.startswith("CHIME"))
            ]
        )
        snrs = np.array([x for x in snrs if not np.isnan(x)])
        return np.percentile(snrs, percentile), np.mean(snrs), 1
    else:
        subprocess.run(
            f"psrstat -c snr,length -j DFTp -Q *{extension} > snrs.txt", shell=True
        )
        _, snrs, lengths = np.genfromtxt("snrs.txt").T
        # Later, we'll need the mean number of subints. More than one subint
        # means lower TOA S/N because we aren't fully scrunching in time.
        mean_nsubint = max(1, 1 + int(np.mean(lengths) / max_subint))
        return np.percentile(snrs, percentile), np.mean(snrs), mean_nsubint


def get_nchan(scrunch_factor, min_nchan=4, nchan_initial=1024):
    scrunch_factor = min(scrunch_factor, nchan_initial // min_nchan)
    if nchan_initial % scrunch_factor != 0:
        print(
            f"error: invalid scrunch_factor! {nchan_initial} not divisible by {scrunch_factor}!"
        )
        exit(1)
    return nchan_initial // scrunch_factor


def parse_email(email_arg):
    email = ""
    if email_arg and ("@" not in email_arg or "." not in email_arg):
        print(f"error: not a valid email: {email_arg}")
    elif email_arg:
        email = f"--mail-user={email_arg} --mail-type=END,FAIL"
    return email
