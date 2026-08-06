#!/usr/bin/env python

"""Diagnostic plotting helpers for CHIRPP timing fits."""

import pint_pal.plot_utils as pu
from pint.utils import dmxparse


def plot_timing_diagnostics(fitter):
    """Save diagnostic plots for a CHIRPP timing fit."""

    pu.plot_residuals_time(
        fitter,
        restype="prefit",
        save=True,
        legend=True,
        title=True,
    )

    pu.plot_residuals_time(
        fitter,
        restype="postfit",
        save=True,
        legend=True,
        title=True,
    )

    pu.plot_residuals_freq(
        fitter,
        restype="postfit",
        save=True,
        legend=True,
        title=True,
    )

    if hasattr(fitter.model, "binary_model_name"):
        pu.plot_residuals_orb(
            fitter,
            restype="postfit",
            save=True,
            legend=True,
            title=True,
        )

    free_dmx_params = [
        param
        for param in fitter.model.free_params
        if str(param).startswith("DMX_")
    ]

    if free_dmx_params:
        try:
            dmx_dict = dmxparse(fitter, save=False)
            pu.plot_dmx_time(
                fitter,
                savedmx=False,
                save=True,
                legend=True,
                title=True,
                dmx=dmx_dict["dmxs"].value,
                errs=dmx_dict["dmx_verrs"].value,
                mjds=dmx_dict["dmxeps"].value,
            )
        except (KeyError, RuntimeError) as exc:
            print(
                f"Skipping DMX plot: fitted DMX covariance is unavailable ({exc})."
            )
    else:
        print("Skipping DMX plot: no DMX parameters were fit.")