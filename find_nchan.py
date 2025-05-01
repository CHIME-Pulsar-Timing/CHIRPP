#!/usr/bin/env python

"""
#########################################################################
##                         nchan determination                         ##
#########################################################################

Determine the nchan to be used in template creation in "pam --setnchn $nchan".
Based on the condition that 75% of frequency scrunched TOAs should pass a S/N > 8 quality cut.
Using the scaling: N_subbands = (x/8)**2 ∝ sqrt(BandWidth) at 25th percentile.

Run from the command line with: 
python find_nchan.py  # Uses the default '*.bmwt.zap'
python find_nchan.py -e clfd # Specify which extension to run on
"""

import numpy as np
import subprocess
import argparse


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


if __name__ == "__main__":
    # Set up argparse to handle the command line input with flag -e
    parser = argparse.ArgumentParser(
        description="Determine scrunch factor based on S/N."
    )
    parser.add_argument(
        "-n",
        "--nchan",
        type=int,
        default=1024,  # Default to 1024 frequency channels
        help="Current number of frequency channels or subbands, by default 1024.",
    )
    parser.add_argument(
        "-m",
        "--min_nchan",
        type=int,
        default=4,  # Default to 4 subbands
        help="Minimum number of subbands to scrunch to, by default 4.",
    )
    parser.add_argument(
        "-s",
        "--snr_threshold",
        type=float,
        default=8.0,
        help="Desired TOA S/N threshold (8.0 by default)",
    )
    parser.add_argument(
        "-p",
        "--pct",
        help="Also return the 'p'-th [0-100] percentile of the S/N distribution",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-e",
        "--extension",
        type=str,
        help="File extension (e.g. '.bmwt.zap') for profiles from which to calculate S/N dist.",
    )
    group.add_argument(
        "-t", "--tim", type=str, help=".tim file with TOAs used for S/N dist."
    )
    parser.add_argument(
        "--max_subint",
        type=float,
        default=3600.0,
        help="Maximum subint duration in seconds (3600.0 by default), if providing a file extension.",
    )
    args = parser.parse_args()

    # Use file extension or .tim file provided via command-line argument
    snr_25pct, snr_mean, mean_nsubint = get_snr_pct(
        25, extension=args.extension, timfile=args.tim
    )

    # We are calculating the S/N after fully scrunching the files,
    # so the effective S/N threshold is multiplied by sqrt(nchan)
    if args.extension:
        snr_threshold = args.snr_threshold * args.nchan**0.5
    else:
        snr_threshold = args.snr_threshold

    # Determine factor by which to scrunch based on the 25th percentile S/N.
    scrunch_factor = get_scrunch_factor(
        snr_25pct, snr_threshold=snr_threshold, mean_nsubint=mean_nsubint
    )

    # Number of subbands to use in "pam --setnchn $nchan"
    # --setnchn: Frequency scrunch to this many subbands
    nchan_scrunched = get_nchan(
        scrunch_factor, min_nchan=args.min_nchan, nchan_initial=args.nchan
    )

    print(f"Mean S/N: {snr_mean:.2f}")

    if args.pct:
        try:
            pct = float(args.pct)
            snr_pct, _, _ = get_snr_pct(pct, extension=args.extension, timfile=args.tim)
            print(f"{pct:.2f}-th percentile S/N: {snr_pct:.2f}")
        except ValueError:
            print(f"\nInvalid entry for -p/--pct: {args.pct}")

    print(f"\n25th-percentile S/N: {snr_25pct:.2f}")
    print(f"Recommended number of subbands: {nchan_scrunched}")
    print(
        f"(scrunch from {args.nchan} subbands by a factor of {scrunch_factor}, to a minimum of {args.min_nchan})\n"
    )
