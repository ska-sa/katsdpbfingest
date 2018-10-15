"""
Interim replacement that moves someway from the static configuration of
RTS to a semi-dynamic model that uses the antenna-mask to add Antenna
components. CBF and other devices still static whilst we develop
kattelmod
"""

from typing import List

from .telescope_model import TelescopeComponent, TelescopeModel


# Component Definitions

class AntennaPositioner(TelescopeComponent):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_sensors(
            ['activity', 'target',
             'pos_request_scan_azim', 'pos_request_scan_elev',
             'pos_actual_scan_azim', 'pos_actual_scan_elev',
             'ap_indexer_position'], True)
        self.add_attributes(['observer'], True)


class CorrelatorBeamformer(TelescopeComponent):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_sensors(['target'], True)
        self.add_sensors(['auto_delay_enabled'], False)


class Enviro(TelescopeComponent):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_sensors(
            ['air_pressure', 'air_relative_humidity', 'air_temperature',
             'mean_wind_speed', 'wind_direction'])


class Observation(TelescopeComponent):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_sensors(['label'], True)
        self.add_sensors(['script_log'], False)


def create_model(antenna_mask: List[str] = []):
    components = []      # type: List[TelescopeComponent]
    for ant_name in antenna_mask:
        components.append(AntennaPositioner(name=ant_name))
    cbf = CorrelatorBeamformer(name='cbf')
    env = Enviro(name='anc')
    obs = Observation(name='obs')
    components.extend([cbf, env, obs])

    model = TelescopeModel()
    model.add_components(components)
    return model
