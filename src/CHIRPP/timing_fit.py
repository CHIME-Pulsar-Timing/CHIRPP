import pint_pal.par_checker as pc
import pint_pal.dmx_utils as du
import pint_pal.lite_utils as lu
import pint_pal.noise_utils as nu
import pint_pal.plot_utils as pu
from pint_pal.timingconfiguration import TimingConfiguration
from pint.fitter import ConvergenceFailure
import pint.fitter
from pint.utils import dmxparse
from astropy.visualization import quantity_support
quantity_support()

config = "J0032+6946.nb.yaml"  # fill in actual path
par_directory = None   # default location
tim_directory = None   # default location
tc = TimingConfiguration(config, par_directory=par_directory, tim_directory=tim_directory)
mo,to = tc.get_model_and_toas(excised=False,usepickle=False)
to.compute_pulse_numbers(mo)
# Ensure DMX windows are calculated properly, set non-binary epochs to the center of the data span
to = du.setup_dmx(mo,to,frequency_ratio=tc.get_fratio(),max_delta_t=tc.get_sw_delay())
lu.center_epochs(mo,to)
fo = tc.construct_fitter(to,mo)
# Check that free-params follow NANOGrav conventions, fit
fo.model.free_params = tc.get_free_params(fo)
lu.check_fit(fo,skip_check=tc.skip_check)

try:
    fo.fit_toas(maxiter=tc.get_niter())
    fo.model.CHI2.value = fo.resids.chi2
except ConvergenceFailure:
    print('Fitter failed to converge.')

lu.write_par(fo,toatype=tc.get_toa_type(),addext='_prenoise',include_date=True)