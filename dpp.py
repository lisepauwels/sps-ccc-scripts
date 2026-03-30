import pjlsa
import numpy as np

with pjlsa.LSAClient(server="sps").java_api():
#with pjlsa.LSAClient(server="next").java_api():
   from cern.lsa.client import ServiceLocator, ContextService, ParameterService, SettingService, TrimService
   from cern.lsa.domain.settings import IncorporationRequest, Contexts, IncorporationSetting, SettingPartEnum, \
   ContextSettingsRequest, Settings
   from cern.lsa.domain.settings.factory import ParametersRequestBuilder
   from cern.lsa.domain.settings.spi.type import IncorporationRangeImpl
   from cern.accsoft.commons.value import Type, ValueFactory
   from cern.accsoft.commons.domain.particletransfers import SpsParticleTransfer
   from cern.lsa.domain.settings.spi import ScalarSetting

setting_service = ServiceLocator.getService(SettingService)
context_service = ServiceLocator.getService(ContextService)
parameter_service = ServiceLocator.getService(ParameterService)
trim_service = ServiceLocator.getService(TrimService)


# print(find_settings(['logical.RDH.10207/K'], "MD_26_L7200_Q20_North_Extraction_2025_V1"))
def find_settings(correctors, cycle):
    lsa_parameters = parameter_service.findParameters(ParametersRequestBuilder.byParameterNames(correctors))
    cycle = context_service.findStandAloneCycle(cycle)
    settings = setting_service.findContextSettings(ContextSettingsRequest.byStandAloneContextAndParameters(cycle, lsa_parameters))
    functions = {}
    for corrector in correctors:
        setting = settings.getParameterSettings(corrector)
        function = Settings.computeContextBeamInValue(cycle, setting, SettingPartEnum.VALUE)
        functions[corrector] = np.vstack([np.array(function.toXArray()[:]), np.array(function.toYArray()[:])])
    return functions


def dp_offset(offset, t_ms, t_start, t_start_plateau, t_end_plateau, t_end,
              cycle, description=None):
    if not offset:
        print("No bump requested")
        return

    cycle = context_service.findStandAloneCycle(cycle)
    bp = Contexts.getFunctionBeamProcessAt(cycle, SpsParticleTransfer.SPSRING, t_ms)

    if description is None:
        description = f"DP={offset:+.6e}"
    incorporation_request = create_incorporation_rule_plateau(t_start=t_start,
                                t_start_plateau=t_start_plateau,
                                t_end_plateau=t_end_plateau, t_end=t_end, bp=bp,
                                cycle=cycle, knob='SpsLowLevelRF/DpOverPOffset#value',
                                knob_group=f'RF BEAM CONTROL',
                                description=description)

    knob = 'SpsLowLevelRF/DpOverPOffset#value'
    # knob = 'SpsLowLevelRF/RadialSteering#value'
    parameter = parameter_service.findParameterByName(knob)
    scalar_increment = ScalarSetting(Type.DOUBLE)
    scalar_increment.setBeamProcess(bp)
    scalar_increment.setParameter(parameter)
    scalar_increment.setTargetValue(ValueFactory.createScalar(Type.DOUBLE, offset))
    incorporation_request.addIncorporationSetting(IncorporationSetting(scalar_increment, t_ms))

    trim_service.incorporate(incorporation_request.build())


def create_incorporation_rule_plateau(t_start, t_start_plateau, t_end_plateau, t_end, bp,
                                      cycle, knob, knob_group, description):
    rise_time = t_start_plateau - t_start
    fall_time = t_end - t_end_plateau
    assert rise_time > 0
    assert fall_time > 0
    rise_time = f'{rise_time}'
    fall_time = f'{fall_time}'
    inc_range = IncorporationRangeImpl(bp.getTypeName(), knob, knob_group, t_start_plateau,
                                t_end_plateau, 'PLATEAUIR', 'PLATEAUIR', rise_time, fall_time)
    return IncorporationRequest.builder() \
        .setContext(cycle) \
        .setRelative(True) \
        .setSettingPart(SettingPartEnum.TARGET) \
        .setDescription(description) \
        .setIncorporationRanges([inc_range])
