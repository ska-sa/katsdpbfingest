"""
Defines the expected and required sensors and attributes for a telescope.
"""

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Attribute(object):
    def __init__(self, name, full_name, critical=False):
        self.name = name
        self.full_name = full_name
        self.critical = critical


class Sensor(object):
    def __init__(self, name, full_name, critical=False, description=None):
        self.name = name
        self.full_name = full_name
        self.critical = critical
        self.description = description


class TelescopeComponent(object):
    def __init__(self, name, proxy_path=None):
        self.name = name
        self.proxy_path = proxy_path if proxy_path is not None else name
        self.sensors = []
        self.attributes = []

    def add_sensors(self, names, critical=False):
        for name in names:
            self.sensors.append(Sensor(
                name, '{0}_{1}'.format(self.proxy_path, name), critical))

    def add_attributes(self, names, critical=False):
        for name in names:
            self.attributes.append(Attribute(
                name, '{0}_{1}'.format(self.proxy_path, name), critical))


class TelescopeModel(object):
    """A static view of the telescope, with no actual data. Data is provided
    by subclasses of :class:`TelescopeModelData`.
    """
    def __init__(self):
        self.components = {}

    @classmethod
    def enable_debug(cls, debug=True):
        if debug:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

    def add_components(self, components):
        for component in components:
            if component in self.components:
                logger.warning("Component name %s is not unique",
                               component.name)
                continue
            self.components[component.name] = component
        logger.debug("Added %d components to model.", len(self.components))


class TelescopeModelData(object):
    """Abstract base class for accessing the values of sensors and attributes.
    Subclasses must implement `get_sensor_values` and `get_attribute_value`.

    .. todo::

        Add a is_valid method that checks whether all critical attributes and
        sensors are present.

    Parameters
    ----------
    model : :class:`TelescopeModel`
        Underlying telescope model
    """

    def __init__(self, model):
        self.model = model

    @property
    def components(self):
        return self.model.components

    def get_attribute_value(self, attribute):
        """Return the value of an attribute.

        Parameters
        ----------
        attribute : :class:`Attribute`
            Attribute to query

        Returns
        -------
        object
            Value of the attribute, or `None` if the attribute is missing
        """
        raise NotImplementedError()

    def get_sensor_values(self, sensor):
        """Return the values of a sensor.

        Parameters
        ----------
        sensor : :class:`Sensor`
            Sensor to query

        Returns
        -------
        list of (timestamp, value, status) tuples
            Recorded values of the sensor, or `None` if the sensor is missing
        """
        raise NotImplementedError()


class TelstateModelData(TelescopeModelData):
    """Retrieves metadata from a telescope model. Sensor values
    prior to a given time are excluded.

    Parameters
    ----------
    model : :class:`katsdpfilewriter.telescope_model.TelescopeModel`
        Underlying model
    telstate : :class:`katsdptelstate.TelescopeState`
        Telescope state containing the metadata
    start_timestamp : float
        Minimum timestamp for sensor queries
    """
    def __init__(self, model, telstate, start_timestamp):
        super(TelstateModelData, self).__init__(model)
        self._telstate = telstate
        self._start_timestamp = start_timestamp

    def get_attribute_value(self, attribute):
        return self._telstate.get(attribute.full_name)

    def get_sensor_values(self, sensor):
        try:
            values = self._telstate.get_range(sensor.full_name,
                                              self._start_timestamp,
                                              include_previous=True)
        except KeyError:
            return None
        if values is None:
            return None
        # Reorder fields, and insert a status of 'nominal' since we don't get
        # any status information from the telescope state
        return [(ts, value, 'nominal') for (value, ts) in values]
