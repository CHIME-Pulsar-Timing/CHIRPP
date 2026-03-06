#!/usr/bin/env python

from pint_pal.lite_utils import *
from pint_pal.noise_utils import *
from pint_pal.par_checker import *
from pint_pal.utils import *
from pint_pal.dmx_utils import *
from pint_pal.timingconfiguration import TimingConfiguration
from pint_pal.outlier_utils import *
from pint_pal.utils import apply_cut_flag, apply_cut_select
from pint_pal.plot_utils import plot_residuals_time
from pint.utils import dmxparse
import argparse

def run_outlier_analysis(config, par_directory=None, tim_directory=None, epochdrop_threads=1, load_pout=None):
    tc = TimingConfiguration(config, par_directory=par_directory, tim_directory=tim_directory)
    using_wideband = tc.get_toa_type() == 'WB'
    if not load_pout: # load raw tims unless otherwise noted
        mo,to = tc.get_model_and_toas(apply_initial_cuts=True)
        to = setup_dmx(mo,to,frequency_ratio=tc.get_fratio(),max_delta_t=tc.get_sw_delay())
    else:
        mo,to = tc.get_model_and_toas(pout_tim_path=load_pout)
        to = setup_dmx(mo,to,frequency_ratio=tc.get_fratio(),max_delta_t=tc.get_sw_delay())
        
    # Run outlier analysis and assign outlier probabilities to TOAs (narrowband only)
    # Skip calculate_pout if starting from pout.tim (load_pout is set)
    if not using_wideband:
        if not load_pout:
            tc.check_outlier()
            calculate_pout(mo, to, tc)       
        make_pout_cuts(mo, to, tc, outpct_threshold=8.0)

    epochalyptica(mo,to,tc,nproc=epochdrop_threads)
    return to, tc, mo

if __name__ == "__main__":
    # Set up argparse to handle command line input
    parser = argparse.ArgumentParser(
        description="Run NANOGrav outlier analysis on provided TOAs."
    )
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        required=True,
        help="Pulsar config (.yaml) file.",
    )
    parser.add_argument(
        "-p",
        "--par_directory",
        type=str,
        default=None,
        help=".par file location (if this option is unused, will use default from config file).",
    )
    parser.add_argument(
        "-t",
        "--tim_directory",
        type=str,
        default=None,
        help=".tim file location (if this option is unused, will use default from config file).",
    )
    parser.add_argument(
        "-t",
        "--epochdrop_threads",
        type=int,
        default=1,
        help="Number of parallel processes to use for tests.",
    )
    parser.add_argument(
        "-o",
        "--pout",
        type=str,
        default=None,
        help=".tim file with pout values already assigned (i.e. if restarting outlier analyses midway through)",
    )
    parser.add_argument(
        "-r",
        "--run_analysis",
        action="store_true",
        default=False,
        help="Run outlier analysis.",
    )
    parser.add_argument(
        "-a",
        "--autorun",
        action="store_true",
        default=False,
        help="Do not make plots.",
    )
    parser.add_argument(
        "--analyze_postfit",
        action="store_true",
        default=False,
        help="Analyze post-fit residuals.",
    )
    
    args = parser.parse_args()
    if args.run_analysis:
        to, tc, mo = run_outlier_analysis(args.config, args.par_directory, args.tim_directory, args.epochdrop_threads, args.load_pout)
    if not args.autorun:
        file_matches, toa_matches = tc.get_investigation_files()
        # Quick breakdown of existing cut flags (automated excision)
        cuts_dict = cut_summary(to,tc)

        # Apply manual cuts and check for redundancies
        tc.manual_cuts(to)
        to = setup_dmx(mo,to,frequency_ratio=tc.get_fratio(),max_delta_t=tc.get_sw_delay())
        
        # More detailed breakdown of cuts by backend
        plot_cuts_all_backends(to, save=True)
        
        # Plot residuals vs. time after auto/manual cuts
        from pint_pal.plot_utils import plot_residuals_time
        fo = tc.construct_fitter(to,mo)
        plot_residuals_time(fo, restype='prefit')
        
        # Plot residuals & highlight manual cuts
        highlight_cut_resids(to,mo,tc,ylim_good=True)
        
        # Fit if you want to analyze postfit residuals
        if args.analyze_postfit:
            fo.model.free_params = tc.get_free_params(fo)
            fo.fit_toas(maxiter=tc.get_niter())
            
            plot_residuals_time(fo, restype='postfit', plotsig = False, avg = False, whitened = True)

        cal_select_list, full_cal_files = display_cal_dropdowns(file_matches, toa_matches)

        read_plot_cal_dropdowns(cal_select_list, full_cal_files)

        badtoas = tc.get_bad_toas()
        if badtoas:
            for bt in badtoas:
                tc.badtoa_info(bt,to)

        # Look at profiles for auto-excised TOAs (outlier10):
        plot_list = display_auto_ex(tc, mo, cutkeys=['outlier10'], plot_type='profile')
        
        # Look at freq. vs. phase for auto-excised TOAs (epochdrop):
        plot_list = display_auto_ex(tc, mo, cutkeys=['epochdrop'], plot_type='GTpd')