import pjlsa
import numpy as np

with pjlsa.LSAClient(server="sps").java_api():
    from cern.lsa.client import ServiceLocator, ContextService, ParameterService, SettingService, TrimService
    from cern.lsa.domain.settings import TrimRequest, ContextSettingsRequest, Settings, SettingPartEnum
    from cern.lsa.domain.settings.factory import ParametersRequestBuilder
    from cern.accsoft.commons.value import ValueFactory

setting_service = ServiceLocator.getService(SettingService)
context_service = ServiceLocator.getService(ContextService)
parameter_service = ServiceLocator.getService(ParameterService)
trim_service = ServiceLocator.getService(TrimService)

# Confirmed via the LSA Settings Management GUI: Parameter group "TUNE", property "Q",
# Device/Property "SPSBEAM/QH" and "SPSBEAM/QV". Both are Function-type parameters
# (a value per time over the cycle).
TUNE_PARAMETERS = {'H': 'SPSBEAM/QH', 'V': 'SPSBEAM/QV'}


def find_tune_settings(cycle, plane='H'):
    """Read back the current Qx/Qy function (time[ms], value) for a cycle."""
    param_name = TUNE_PARAMETERS[plane]
    lsa_parameters = parameter_service.findParameters(ParametersRequestBuilder.byParameterNames([param_name]))
    cycle_ctx = context_service.findStandAloneCycle(cycle)
    settings = setting_service.findContextSettings(ContextSettingsRequest.byStandAloneContextAndParameters(cycle_ctx, lsa_parameters))
    setting = settings.getParameterSettings(param_name)
    function = Settings.computeContextBeamInValue(cycle_ctx, setting, SettingPartEnum.TARGET)
    return np.vstack([np.array(function.toXArray()[:]), np.array(function.toYArray()[:])])


def value_at(cycle, plane, t_ms):
    """Linearly interpolate the current Qx/Qy function at a given cycle time [ms]. Useful for sanity checks."""
    t, v = find_tune_settings(cycle, plane)
    return float(np.interp(t_ms, t, v))


def set_tune_function(times_ms, values, cycle, plane='H', description=None):
    param_name = TUNE_PARAMETERS[plane]
    cycle_ctx = context_service.findStandAloneCycle(cycle)
    parameter = parameter_service.findParameterByName(param_name)
    function = ValueFactory.createFunction([float(t) for t in times_ms], [float(v) for v in values])
    if description is None:
        description = f"Set {param_name} function"
    trim_request = TrimRequest.builder() \
        .setContext(cycle_ctx) \
        .addFunction(parameter, function) \
        .setDescription(description) \
        .setSettingPart(SettingPartEnum.TARGET) \
        .build()
    trim_service.trimSettings(trim_request)


def set_tune_flat(value, cycle, plane='H', description=None):
    """Set Qx/Qy to a single flat absolute value across the whole cycle."""
    if plane == 'H':
        vals = [26.62007235, 26.62007235, 25.1223633642]
    elif plane == 'V':
        vals = [26.58007223, 26.58007223, 25.1031324846]
    else:
        raise ValueError(f"Wrong plane {plane} for tune trim")
    set_tune_function([0, 35, 50, 100, 3900], [*vals, value, value], cycle, plane, description)


def set_tune_ramp(value_start, value_end, cycle, plane='H', description=None):
    """Ramp Qx/Qy over the cycle."""
    if plane == 'H':
        vals = [26.62007235, 26.62007235, 25.1223633642]
    elif plane == 'V':
        vals = [26.58007223, 26.58007223, 25.1031324846]
    else:
        raise ValueError(f"Wrong plane {plane} for tune trim")
    set_tune_function([0, 35, 50, 100, 1115, 3415], [*vals, value_start, value_start, value_end], cycle, plane, description)


