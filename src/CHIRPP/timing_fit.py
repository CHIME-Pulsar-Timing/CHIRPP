#!/usr/bin/env python

import pint_pal.par_checker as pc
import pint_pal.dmx_utils as du
import pint_pal.lite_utils as lu
# import pint_pal.noise_utils as nu
import pint_pal.plot_utils as pu
from pint_pal.timingconfiguration import TimingConfiguration
from pint.fitter import ConvergenceFailure
# import pint.fitter
# from pint.utils import dmxparse
from astropy.visualization import quantity_support

quantity_support()


def update_timing(config, par_directory=None, tim_directory=None, plots=False):
    tc = TimingConfiguration(
        config, par_directory=par_directory, tim_directory=tim_directory
    )
    mo, to = tc.get_model_and_toas(excised=False, usepickle=False)
    to.compute_pulse_numbers(mo)
    # Ensure DMX windows are calculated properly, set non-binary epochs to the center of the data span
    to = du.setup_dmx(
        mo, to, frequency_ratio=tc.get_fratio(), max_delta_t=tc.get_sw_delay()
    )
    lu.center_epochs(mo, to)
    fo = tc.construct_fitter(to, mo)
    fo.model.free_params = tc.get_free_params(fo)
    # lu.check_fit(fo, skip_check=tc.skip_check)
    try:
        fo.fit_toas(maxiter=tc.get_niter())
        fo.model.CHI2.value = fo.resids.chi2
    except ConvergenceFailure:
        print("Fitter failed to converge.")
    lu.write_par(fo, toatype=tc.get_toa_type(), addext="_prenoise", include_date=True)
    if plots:
        pu.plot_residuals(fo, to, tc.get_toa_type(), title="Post-Fit Residuals")
        pu.plot_dmx(fo, to, tc.get_toa_type(), title="Post-Fit DMX")
        if pc.check_binary(fo.model):
            pu.plot_binary(fo, to, tc.get_toa_type(), title="Post-Fit Residuals")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Update timing model with new DMX windows and free parameters."
    )
    parser.add_argument("config", help="Path to the timing configuration file (.yaml).")
    parser.add_argument(
        "--par_directory",
        help="Directory containing .par file. If not provided, uses the directory of the config file.",
        default=None,
    )
    parser.add_argument(
        "--tim_directory",
        help="Directory containing .tim file(s). If not provided, uses the directory of the config file.",
        default=None,
    )
    parser.add_argument(
        "--plots",
        help="Generate residual & DMX plots after fitting.",
        action="store_true",
    )
    args = parser.parse_args()

    update_timing(
        args.config,
        par_directory=args.par_directory,
        tim_directory=args.tim_directory,
        plots=args.plots,
    )
