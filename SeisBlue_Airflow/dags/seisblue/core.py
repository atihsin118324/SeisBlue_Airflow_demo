from dataclasses import dataclass, field
from typing import Optional, Union
import numpy as np
from datetime import datetime
from typing import List
import sqlalchemy.ext.declarative
from sqlalchemy.dialects.mysql import DATETIME
import json
from types import SimpleNamespace

from seisblue import tool

Base = sqlalchemy.ext.declarative.declarative_base()


@dataclass
class TimeWindow:
    starttime: Optional[Union[datetime, str]] = None
    endtime: Optional[Union[datetime, str]] = None
    npts: Optional[int] = None
    samplingrate: Optional[float] = None
    delta: Optional[float] = None


@dataclass
class Inventory:
    network: Optional[str] = None
    station: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    elevation: Optional[float] = None
    timewindow: Optional[TimeWindow] = None
    header: dict = field(repr=False, default=None)


@dataclass
class Event:
    time: Optional[Union[datetime, str]] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    depth: Optional[float] = None
    magnitude: Optional[float] = None
    header: dict = field(repr=False, default=None)


@dataclass
class Pick:
    time: Optional[Union[datetime, str]] = None
    inventory: Optional[Inventory] = None
    phase: Optional[str] = None
    tag: Optional[str] = None
    snr: Optional[float] = None
    traceid: Optional[str] = None
    confidence: Optional[float] = None
    header: dict = field(repr=False, default=None)


@dataclass
class Trace:
    inventory: Optional[Inventory] = None
    timewindow: Optional[TimeWindow] = None
    channel: Optional[str] = None
    data: Optional[np.ndarray] = field(default=None)
    header: dict = field(repr=False, default=None)


@dataclass
class Label:
    inventory: Optional[Inventory] = None
    timewindow: Optional[TimeWindow] = None
    phase: Optional[str] = None
    tag: Optional[str] = None
    data: np.ndarray = field(default=None)
    picks: List[Pick] = field(default_factory=list)


@dataclass
class Instance:
    inventory: Optional[Inventory] = None
    timewindow: Optional[TimeWindow] = None
    traces: Optional[List[Trace]] = field(default_factory=list)
    labels: Optional[List[Label]] = field(default_factory=list)
    id: Optional[str] = None
    dataset: Optional[str] = None
    datasetpath: Optional[str] = None

    @staticmethod
    def from_json(obj):
        item = json.loads(obj, object_hook=lambda d: SimpleNamespace(**d))
        return Instance(item)

    def to_json(self):
        return json.dumps(self, cls=tool.EnhancedEncoder)


class InventorySQL(Base):
    """
    Inventory table for sql database.
    """
    __tablename__ = 'inventory'
    id = sqlalchemy.Column(sqlalchemy.BigInteger()
                           .with_variant(sqlalchemy.Integer, "sqlite"),
                           primary_key=True)
    network = sqlalchemy.Column(sqlalchemy.String(6), nullable=False)
    station = sqlalchemy.Column(sqlalchemy.String(6), nullable=False)
    latitude = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    longitude = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    elevation = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    starttime = sqlalchemy.Column(DATETIME(fsp=2), nullable=True)
    endtime = sqlalchemy.Column(DATETIME(fsp=2), nullable=True)

    def __init__(self, geom):
        self.network = geom.network
        self.station = geom.station
        self.latitude = geom.latitude
        self.longitude = geom.longitude
        self.elevation = geom.elevation
        if geom.timewindow:
            self.starttime = geom.timewindow.starttime
            self.endtime = geom.timewindow.endtime
        else:
            self.starttime = None
            self.endtime = None

    def __repr__(self):
        return (
            f"Inventory("
            f"Network={self.network}, "
            f"Station={self.station}, "
            f"Latitude={self.latitude:>7.4f}, "
            f"Longitude={self.longitude:>8.4f}, "
            f"Elevation={self.elevation:>6.1f})"
        )

    def to_dataclass(self):
        inventory = Inventory(
            network=self.network,
            station=self.station,
            latitude=self.latitude,
            longitude=self.longitude,
            elevation=self.elevation,
        )
        if self.starttime and self.endtime:
            inventory.timewindow = TimeWindow(
                starttime=datetime.fromisoformat(str(self.starttime)),
                endtime=datetime.fromisoformat(str(self.endtime)),
            )
        return inventory


class EventSQL(Base):
    """
    Event table for sql database.
    """
    __tablename__ = 'event'
    id = sqlalchemy.Column(sqlalchemy.BigInteger()
                           .with_variant(sqlalchemy.Integer, "sqlite"),
                           primary_key=True)
    time = sqlalchemy.Column(DATETIME(fsp=2), nullable=False)
    latitude = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    longitude = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    depth = sqlalchemy.Column(sqlalchemy.Float, nullable=False)
    magnitude = sqlalchemy.Column(sqlalchemy.Float, nullable=True)

    def __init__(self, event):
        self.time = event.time
        self.latitude = event.latitude
        self.longitude = event.longitude
        self.depth = event.depth
        self.magnitude = event.magnitude if event.magnitude else None

    def __repr__(self):
        return (
            f"Event("
            f"Time={self.time}"
            f"Latitude={self.latitude:>7.4f}, "
            f"Longitude={self.longitude:>8.4f}, "
            f"Depth={self.depth:>6.1f,},"
            f"Magnitude={self.depth:>6.1f,})"
        )


class PickSQL(Base):
    """
    Pick table for sql database.
    """
    __tablename__ = 'pick'
    id = sqlalchemy.Column(sqlalchemy.BigInteger()
                           .with_variant(sqlalchemy.Integer, "sqlite"),
                           primary_key=True)
    time = sqlalchemy.Column(DATETIME(fsp=2), nullable=False)
    network = sqlalchemy.Column(sqlalchemy.String(6))
    station = sqlalchemy.Column(sqlalchemy.String(6),
                                nullable=False)
    phase = sqlalchemy.Column(sqlalchemy.String(90), nullable=False)
    tag = sqlalchemy.Column(sqlalchemy.String(10), nullable=False)
    snr = sqlalchemy.Column(sqlalchemy.Float)
    confidence = sqlalchemy.Column(sqlalchemy.Float)

    def __init__(self, pick):
        self.time = pick.time
        self.station = pick.inventory.station
        self.network = pick.inventory.network
        self.phase = pick.phase
        self.tag = pick.tag
        self.snr = None
        self.confidence = None

    def __repr__(self):
        return (
            f"Pick("
            f"Time={self.time}, "
            f"Station={self.station}, "
            f"Phase={self.phase}, "
            f"Tag={self.tag}, "
            f"snr={self.snr}, "
            f"confidence={self.confidence},"
            f"id={self.id})"
        )

    def to_dataclass(self):
        pick = Pick(
            time=datetime.fromisoformat(str(self.time)),
            phase=self.phase,
            tag=self.tag,
            inventory=Inventory(station=self.station, network=self.network),
            snr=self.snr,
            confidence=self.confidence
        )
        return pick


class WaveformSQL(Base):
    """
    Waveform table for sql database.
    """

    __tablename__ = "waveform"
    id = sqlalchemy.Column(sqlalchemy.String(180), primary_key=True)
    starttime = sqlalchemy.Column(DATETIME(fsp=2), nullable=False)
    endtime = sqlalchemy.Column(DATETIME(fsp=2), nullable=False)
    station = sqlalchemy.Column(sqlalchemy.String(6), nullable=False)
    network = sqlalchemy.Column(sqlalchemy.String(6), nullable=True)
    channel = sqlalchemy.Column(sqlalchemy.String(20), nullable=False)
    dataset = sqlalchemy.Column(sqlalchemy.String(20), nullable=False)
    datasetpath = sqlalchemy.Column(sqlalchemy.String(180), nullable=False)

    def __init__(self, instance):
        self.starttime = instance.timewindow.starttime
        self.endtime = instance.timewindow.endtime
        self.station = instance.inventory.station
        self.network = instance.inventory.network
        self.channel = ", ".join([tr.channel for tr in instance.traces])
        self.dataset = instance.dataset
        self.datasetpath = instance.datasetpath
        self.id = instance.id

    def __repr__(self):
        return (
            f"Waveform("
            f"Start={self.starttime}, "
            f"End={self.endtime}, "
            f"Network={self.network}, "
            f"Station={self.station}, "
            f"Channel={self.channel}, "
            f"Dataset={self.dataset}, "
            f"DatasetPath={self.datasetpath}, "
            f"Id={self.id})"
        )


@dataclass
class PickAssoc:
    sta: Optional[str] = None
    net: Optional[str] = None
    loc: Optional[str] = None
    time: Optional[Union[datetime, str]] = None
    snr: Optional[float] = None
    phase: Optional[str] = None
    locate_flag: Optional[bool] = None
    assoc_id: Optional[int] = None
    trace_id: Optional[str] = None


class PickAssocSQL(Base):
    __tablename__ = "pick_assoc"
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    sta = sqlalchemy.Column(sqlalchemy.String(5), nullable=False)
    net = sqlalchemy.Column(sqlalchemy.String(2), nullable=True)
    loc = sqlalchemy.Column(sqlalchemy.String(2), nullable=True)
    time = sqlalchemy.Column(DATETIME(fsp=2), nullable=False)
    snr = sqlalchemy.Column(sqlalchemy.Float)
    phase = sqlalchemy.Column(sqlalchemy.String(5), nullable=False)
    locate_flag = sqlalchemy.Column(sqlalchemy.Boolean)
    assoc_id = sqlalchemy.Column(sqlalchemy.Integer)
    trace_id = sqlalchemy.Column(sqlalchemy.String(20))

    def __init__(self, pick):
        self.sta = pick.sta
        self.net = "" if not pick.net else pick.net
        self.loc = "" if not pick.loc else pick.loc
        self.time = pick.time
        self.snr = pick.snr
        self.phase = pick.phase
        self.locate_flag = None
        self.assoc_id = None
        self.trace_id = pick.trace_id

    def __repr__(self):
        return (
            f"Pick("
            f"{self.sta}, "
            f"{self.net}, "
            f"{self.loc}, "
            f'{self.time.isoformat("T")},'
            f"{self.phase}, "
            f"assoc_id: {self.assoc_id}, "
            f"trace_id:{self.trace_id})"
        )


class Candidate(Base):
    __tablename__ = "candidate"
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    origin_time = sqlalchemy.Column(sqlalchemy.DateTime)
    sta = sqlalchemy.Column(sqlalchemy.String(5))
    weight = sqlalchemy.Column(sqlalchemy.Float)
    p_time = sqlalchemy.Column(sqlalchemy.DateTime)
    p_id = sqlalchemy.Column(sqlalchemy.Integer)
    s_time = sqlalchemy.Column(sqlalchemy.DateTime)
    s_id = sqlalchemy.Column(sqlalchemy.Integer)
    locate_flag = sqlalchemy.Column(sqlalchemy.Boolean)
    assoc_id = sqlalchemy.Column(sqlalchemy.Integer)

    def __init__(self, origin_time, sta, p_time, p_id, s_time, s_id):
        self.origin_time = origin_time
        self.sta = sta
        self.weight = None
        self.p_time = p_time
        self.s_time = s_time
        self.p_id = p_id
        self.s_id = s_id
        self.locate_flag = None
        self.assoc_id = None

    def __repr__(self):
        return (
            f"Candidate Event("
            f'{self.origin_time.isoformat("T")}, '
            f"{self.sta}, "
            f"p_id: {self.p_id}, "
            f"s_id: {self.s_id})"
        )

    def set_assoc_id(self, assoc_id, session, locate_flag):
        self.assoc_id = assoc_id
        self.locate_flag = locate_flag
        """
        Assign phases to modified picks
        """
        pick_p = session.query(PickAssoc).filter(PickAssoc.id == self.p_id)
        for pick in pick_p:
            pick.phase = "P"
            pick.assoc_id = assoc_id
            pick.locate_flag = locate_flag

        pick_s = session.query(PickAssoc).filter(PickAssoc.id == self.s_id)
        for pick in pick_s:
            pick.phase = "S"
            pick.assoc_id = assoc_id
            pick.locate_flag = locate_flag


class AssociatedEvent(Base):
    __tablename__ = "associated"
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    origin_time = sqlalchemy.Column(sqlalchemy.DateTime)
    time_std = sqlalchemy.Column(sqlalchemy.Float)
    latitude = sqlalchemy.Column(sqlalchemy.Float)
    longitude = sqlalchemy.Column(sqlalchemy.Float)
    depth = sqlalchemy.Column(sqlalchemy.Float)
    nsta = sqlalchemy.Column(sqlalchemy.Integer)
    rms = sqlalchemy.Column(sqlalchemy.Float)
    erln = sqlalchemy.Column(sqlalchemy.Float)
    erlt = sqlalchemy.Column(sqlalchemy.Float)
    erdp = sqlalchemy.Column(sqlalchemy.Float)
    gap = sqlalchemy.Column(sqlalchemy.Float)

    def __init__(
            self,
            origin_time,
            time_std,
            latitude,
            longitude,
            depth,
            nsta,
            rms,
            erln,
            erlt,
            erdp,
            gap,
    ):
        self.origin_time = origin_time
        self.time_std = time_std
        self.latitude = latitude
        self.longitude = longitude
        self.depth = depth
        self.nsta = nsta
        self.rms = rms
        self.erln = erln
        self.erlt = erlt
        self.erdp = erdp
        self.gap = gap

    def __repr__(self):
        return (
            f"Associated Event("
            f'{self.origin_time.isoformat("T")}, '
            f"lat: {self.latitude:.3f}, "
            f"lon: {self.longitude:.3f}, "
            f"dep: {self.depth:.3f}, "
            f"nsta: {self.nsta}, "
        )




model_dict = {
    'inventory': Inventory,
    'timewindow': TimeWindow,
    'traces': Trace,
    'labels': Label,
    'label': Label,
    'pick': Pick,
    'picks': Pick,
    'instance': Instance,
}


if __name__ == '__main__':
    pass