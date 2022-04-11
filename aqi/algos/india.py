# -*- coding: utf-8 -*-

from decimal import *

from aqi.constants import (POLLUTANT_PM25, POLLUTANT_PM10,
                          POLLUTANT_O3_8H, POLLUTANT_O3_1H,
                          POLLUTANT_CO_8H, POLLUTANT_SO2_1H,
                          POLLUTANT_NO2_1H)
from aqi.algos.base import PiecewiseAQI


class AQI(PiecewiseAQI):
    """Implementation of the EPA AQI algorithm.
    """

    piecewise = {
        'aqi': [
            (0, 50),
            (51, 100),
            (101, 200),
            (201, 300),
            (301, 400),
            (401, 500)],
        'bp': {
            POLLUTANT_O3_8H: [
                (Decimal('0.000'), Decimal('50')),
                (Decimal('51'), Decimal('100')),
                (Decimal('101'), Decimal('168')),
                (Decimal('169'), Decimal('208')),
                (Decimal('209'), Decimal('748')),
                (Decimal('749'), Decimal('1000'))
            ],
            POLLUTANT_PM10: [
                (Decimal('0'), Decimal('50')),
                (Decimal('51'), Decimal('100')),
                (Decimal('101'), Decimal('250')),
                (Decimal('251'), Decimal('350')),
                (Decimal('351'), Decimal('430')),
                (Decimal('431'), Decimal('1000'))
            ],
            POLLUTANT_PM25: [
                (Decimal('0.0'), Decimal('30.0')),
                (Decimal('31'), Decimal('60')),
                (Decimal('61'), Decimal('90')),
                (Decimal('91'), Decimal('120')),
                (Decimal('121'), Decimal('250')),
                (Decimal('251'), Decimal('1000')),
            ]
        },
        'prec': {
            POLLUTANT_O3_8H: Decimal('1.'),
            POLLUTANT_PM10: Decimal('1.'),
            POLLUTANT_PM25: Decimal('1.'),
        },
        'units': {
            POLLUTANT_O3_8H: 'ppm',
            POLLUTANT_PM10: 'µg/m³',
            POLLUTANT_PM25: 'µg/m³',
        },
    }
