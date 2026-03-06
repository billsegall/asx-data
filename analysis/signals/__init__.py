from .base import Signal
from .short_trend import ShortTrendSignal
from .short_squeeze import ShortSqueezeSignal
from .volume_anomaly import VolumeAnomalySignal
from .commodity_lead import CommodityLeadSignal
from .announcement import AnnouncementSignal

ALL_SIGNALS = [ShortTrendSignal(), ShortSqueezeSignal(), VolumeAnomalySignal()]
