from seisblue import core, tool

import operator
from datetime import datetime
import sqlalchemy
import sqlalchemy.ext.declarative
import contextlib
from tqdm import tqdm
from pymysql.err import OperationalError


class Client:
    """
    Client for sql database
    """

    def __init__(self, database, echo=False, build=False):
        self.database = database
        db_path = database
        username = "root"
        password = "sgylab"
        host = ""
        port = ""

        if build:
            self.engine = sqlalchemy.create_engine(
                f"mysql+pymysql://{username}:{password}@{host}:{port}", echo=echo
            )
            self.engine.execute(f"CREATE DATABASE {database}")  # create db
            self.engine.execute(f"USE {database}")
            core.Base.metadata.create_all(bind=self.engine)
        try:
            self.engine = sqlalchemy.create_engine(
                f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}",
                echo=echo,
                pool_size=300,
                max_overflow=-1,
            )
            self.session = sqlalchemy.orm.sessionmaker(bind=self.engine)
        except Exception as err:
            print(err)
            raise FileNotFoundError(f"{db_path} is not found")

    def __repr__(self):
        return f"SQL Database({self.database})"

    def show_table_names(self):
        insp = sqlalchemy.inspect(self.engine)
        print(f"Table names : {insp.get_table_names()}")

    def add_inventory(self, objs, remove_duplicates=True):
        with self.session_scope() as session:
            core.InventorySQL.__table__.create(bind=session.get_bind(), checkfirst=True)
            try:
                session.bulk_save_objects(objs)
                print(f'Add {len(objs)} inventory into database.')
            except OperationalError as e:
                print(e)
        if remove_duplicates:
            self.remove_duplicates('inventory', ['network', 'station', 'latitude', 'longitude', 'elevation'])

    def add_events(self, objs, remove_duplicates=True):
        with self.session_scope() as session:
            core.EventSQL.__table__.create(bind=session.get_bind(), checkfirst=True)
            try:
                session.bulk_save_objects(objs)
                print(f'Add {len(objs)} events into database.')
            except OperationalError as e:
                print(e)
        if remove_duplicates:
            self.remove_duplicates('event', ['time', 'latitude', 'longitude', 'depth'])

    def add_picks(self, objs, remove_duplicates=True):
        with self.session_scope() as session:
            try:
                session.bulk_save_objects(objs)
                print(f'Add {len(objs)} picks into database.')
            except OperationalError as e:
                print(e)
        if remove_duplicates:
            self.remove_duplicates('pick', ['time', 'network', 'station', 'phase', 'tag'])

    def add_waveforms(self, objs):
        with self.session_scope() as session:
            core.WaveformSQL.__table__.create(bind=session.get_bind(), checkfirst=True)
            try:
                session.bulk_save_objects(objs)
                print(f'Add {len(objs)} waveforms into database.')
            except OperationalError as e:
                print(e)

    def add_pickassoc(self, objs):
        with self.session_scope() as session:
            core.PicksAssoc.__table__.create(bind=session.get_bind(), checkfirst=True)
            session.bulk_save_objects(objs)
            session.commit()
            index_sql = sqlalchemy.DDL('CREATE INDEX idx_time ON pick_assoc (time)')
            session.execute(index_sql)
            session.commit()

        print(f'Add {len(objs)} pickassoc into database.')

    def add_magnitude_into_event(self, event):
        with self.session_scope() as session:
            session.query(event).get(event.id).update({'magnitude': event.magnitude}, synchronize_session=False)


    @staticmethod
    def get_table_class(table):
        """
        Returns related class from dict.

        :param str table: Keywords: inventory, event, pick, waveform.
        :return: SQL table class.
        """
        table_dict = {
            "inventory": core.InventorySQL,
            "event": core.EventSQL,
            "pick": core.PickSQL,
            "waveform": core.WaveformSQL,
        }
        try:
            table_class = table_dict.get(table)
            return table_class

        except KeyError:
            msg = "Please select table: inventory, event, pick, waveform"
            raise KeyError(msg)

    def remove_duplicates(self, table, match_columns):
        """
        Removes duplicates data in given table.

        :param str table: Target table.
        :param list match_columns: List of column names.
            If all columns matches, then marks it as a duplicate data.
        """
        table = self.get_table_class(table)
        with self.session_scope() as session:
            attrs = operator.attrgetter(*match_columns)
            table_columns = attrs(table)
            distinct = session \
                .query(sqlalchemy.func.min(table.id)) \
                .group_by(*table_columns) \
                .subquery()
            duplicate = session \
                .query(table) \
                .filter(table.id.notin_(session.query(distinct))) \
                .delete(synchronize_session='fetch')

            if duplicate:
                print(f'Remove {duplicate} duplicate {table.__tablename__}s')

    def get_inventory(self, **kwargs):
        """
        Returns query from inventory table.
        :param session
        :rtype: sqlalchemy.orm.query.Query
        :return: A Query.
        """
        inventory = core.InventorySQL
        with self.session_scope() as session:
            query = session.query(inventory)
            for key, value in kwargs.items():
                if key == 'network':
                    value = [value] if isinstance(value, str) else value
                    query = query.filter(inventory.network.in_(value))
                if key == 'station':
                    value = [value] if isinstance(value, str) else value
                    query = query.filter(inventory.station.in_(value))
                if key == 'time':
                    value = datetime.fromisoformat(value) if isinstance(value, str) else value
                    query = query.filter(
                        sqlalchemy.and_(
                            value >= inventory.starttime,
                            value <= inventory.endtime
                        )
                    )
        return query.first()

    def get_pick(self, with_inventory=False, **kwargs):
        pick = core.PickSQL
        inventory = core.InventorySQL
        with self.session_scope() as session:
            if with_inventory:
                query = session.query(pick, inventory).join(inventory, pick.station == inventory.station)
            else:
                query = session.query(pick)
            for key, value in kwargs.items():
                if key == 'from_time':
                    value = datetime.fromisoformat(value) if isinstance(value, str) else value
                    query = query.filter(pick.time >= value)
                elif key == 'to_time':
                    value = datetime.fromisoformat(value) if isinstance(value, str) else value
                    query = query.filter(pick.time <= value)
                elif key == 'network':
                    value = [value] if isinstance(value, str) else value
                    query = query.filter(pick.network.in_(value))
                elif key == 'station':
                    value = [value] if isinstance(value, str) else value
                    query = query.filter(pick.station.in_(value))
                elif key == 'phase':
                    value = [value] if isinstance(value, str) else value
                    query = query.filter(pick.phase.in_(value))
                elif key == 'tag':
                    value = [value] if isinstance(value, str) else value
                    query = query.filter(pick.tag.in_(value))
                elif key == 'low_snr':
                    query = query.filter(pick.snr >= value)
                elif key == 'high_snr':
                    query = query.filter(pick.snr <= value)
                elif key == 'low_confidence':
                    query = query.filter(pick.confidence >= value)
                elif key == 'high_confidence':
                    query = query.filter(pick.confidence <= value)
        return query.all()

    def get_event(
            self,
            from_time=None,
            to_time=None,
            min_longitude=None,
            max_longitude=None,
            min_latitude=None,
            max_latitude=None,
            min_depth=None,
            max_depth=None,
    ):
        with self.session_scope() as session:
            query = session.query(core.EventSQL)
            if from_time is not None:
                query = query.filter(core.EventSQL.time >= from_time)
            if to_time is not None:
                query = query.filter(core.EventSQL.time <= to_time)
            if min_longitude is not None:
                query = query.filter(core.EventSQL.longitude >= min_longitude)
            if max_longitude is not None:
                query = query.filter(core.EventSQL.longitude <= max_longitude)
            if min_latitude is not None:
                query = query.filter(core.EventSQL.latitude >= min_latitude)
            if max_latitude is not None:
                query = query.filter(core.EventSQL.latitude <= max_latitude)
            if min_depth is not None:
                query = query.filter(core.EventSQL.depth >= min_depth)
            if max_depth is not None:
                query = query.filter(core.EventSQL.depth <= max_depth)
        return query.all()

    def get_waveform(self, **kwargs):
        waveform = core.WaveformSQL
        with self.session_scope() as session:
            query = session.query(core.WaveformSQL)
            for key, value in kwargs.items():
                if key == 'time':
                    query = query.filter(
                        sqlalchemy.and_(
                            value >= core.WaveformSQL.starttime,
                            value <= core.WaveformSQL.endtime
                        )
                    )
                elif key == 'from_time':
                    value = datetime.fromisoformat(value) if isinstance(value, str) else value
                    query = query.filter(waveform.starttime >= value)
                elif key == 'to_time':
                    value = datetime.fromisoformat(value) if isinstance(value, str) else value
                    query = query.filter(waveform.starttime <= value)
                elif key == 'station':
                    value = [value] if isinstance(value, str) else value
                    query = query.filter(core.WaveformSQL.station.in_(value))
                elif key == 'network':
                    value = [value] if isinstance(value, str) else value
                    query = query.filter(core.WaveformSQL.network.in_(value))
                elif key == 'dataset':
                    value = [value] if isinstance(value, str) else value
                    query = query.filter(core.WaveformSQL.dataset.in_(value))
        return query.all()

    def get_associates(
            self,
            from_time=None,
            to_time=None,
            min_longitude=None,
            max_longitude=None,
            min_latitude=None,
            max_latitude=None,
            min_depth=None,
            max_depth=None,
            min_erlim=None,
            max_erlim=None,
            min_sta=None,
            max_sta=None,
            max_ot_uncert=None,
            gap=None,
    ):
        with self.session_scope() as session:
            query = session.query(core.AssociatedEvent)
            if from_time is not None:
                query = query.filter(core.AssociatedEvent.origin_time >= from_time)
            if to_time is not None:
                query = query.filter(core.AssociatedEvent.origin_time <= to_time)
            if min_longitude is not None:
                query = query.filter(core.AssociatedEvent.longitude >= min_longitude)
            if max_longitude is not None:
                query = query.filter(core.AssociatedEvent.longitude <= max_longitude)
            if min_latitude is not None:
                query = query.filter(core.AssociatedEvent.latitude >= min_latitude)
            if max_latitude is not None:
                query = query.filter(core.AssociatedEvent.latitude <= max_latitude)
            if min_depth is not None:
                query = query.filter(core.AssociatedEvent.depth >= min_depth)
            if max_depth is not None:
                query = query.filter(core.AssociatedEvent.depth <= max_depth)
            if min_erlim is not None:
                query = query.filter(
                    (core.AssociatedEvent.erlt > min_erlim)
                    | (core.AssociatedEvent.erln > min_erlim)
                    | (core.AssociatedEvent.erdp > min_erlim)
                )
            if max_erlim is not None:
                query = query.filter(core.AssociatedEvent.erlt <= max_erlim)
                query = query.filter(core.AssociatedEvent.erln <= max_erlim)
                query = query.filter(core.AssociatedEvent.erdp <= max_erlim)
            if min_sta is not None:
                query = query.filter(core.AssociatedEvent.nsta >= min_sta)
            if max_sta is not None:
                query = query.filter(core.AssociatedEvent.nsta <= max_sta)
            if max_ot_uncert is not None:
                query = query.filter(core.AssociatedEvent.time_std <= max_ot_uncert)
            if gap is not None:
                query = query.filter(core.AssociatedEvent.gap <= gap)

        return query.all()

    def get_candidate(self, assoc_id=None):
        with self.session_scope() as session:
            query = session.query(core.Candidate)
            if id is not None:
                query = query.filter(core.Candidate.assoc_id == assoc_id)

        return query.all()

    def get_pick_assoc_id(self, assoc_id):
        with self.session_scope() as session:
            query = session.query(core.PicksAssoc).filter(core.PicksAssoc.assoc_id == assoc_id)
        return query.all()

    @contextlib.contextmanager
    def session_scope(self):
        """
        Provide a transactional scope around a series of operations.
        """
        session = self.session()
        try:
            yield session
            session.commit()
        except Exception as exception:
            print(f"{exception.__class__.__name__}: {exception.__cause__}")
            session.rollback()
        finally:
            session.close()


if __name__ == '__main__':
    pass
