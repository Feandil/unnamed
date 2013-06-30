# ----------------------------------------------------------------------------
# "THE BEER-WARE LICENSE" (Revision 42):
# <git@lerya.net> wrote this file. As long as you retain this notice you can
# do whatever you want with this stuff. If we meet some day, and you think
# this stuff is worth it, you can buy me a beer in return. Vincent Brillault
# ----------------------------------------------------------------------------

"""Database backend for the whole project"""

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (Boolean,
                        Column,
                        ForeignKey,
                        Index,
                        Integer,
                        String,
                        UniqueConstraint
                        )
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, relationship


BASE = declarative_base()


class DBFile(BASE):  # pylint: disable=R0903,W0232
    """A single file as define in remote database"""
    __tablename__ = 'data_file'

    id = Column(Integer, primary_key=True)
    crc = Column(String(8))


class HashFile(BASE):  # pylint: disable=R0903,W0232
    """Hashed file information"""
    __tablename__ = 'hash_file'

    id = Column(Integer, primary_key=True)
    crc = Column(String(8))
    e2dk = Column(String(32))
    lastcheck = Column(Integer)
    content_id = Column(Integer, ForeignKey('data_file.id'))
    manual = Column(Boolean, default=False)
    content = relationship("DBFile")

    __table_args__ = (UniqueConstraint('crc', 'e2dk'),
                      Index('hash_file_index', 'crc', 'e2dk'))


class PathRoot(BASE):  # pylint: disable=R0903,W0232
    """Roots of the path being monitored"""
    __tablename__ = 'path_root'

    id = Column(Integer, primary_key=True)
    path = Column(String(100), unique=True)


class PathFile(BASE):  # pylint: disable=R0903,W0232
    """Files present in the filesystem and monitored"""
    __tablename__ = 'path_file'

    id = Column(Integer, primary_key=True)
    root_id = Column(Integer, ForeignKey('path_root.id'))
    name = Column(String())
    path = Column(String())
    hash_id = Column(Integer, ForeignKey('hash_file.id'))
    root = relationship("PathRoot")
    hash = relationship("HashFile")

    __table_args__ = (UniqueConstraint('name', 'path'),
                      Index('path_file_index', 'name', 'path'))


class SessionFactory(object):  # pylint: disable=R0903
    """Small wrapper on top of sqlalchemy"""

    def __init__(self, database=None, echo=False):
        engine = create_engine(database, echo=echo)
        BASE.metadata.create_all(engine)
        self._factory = sessionmaker(bind=engine)

    def get(self, **kwargs):
        """Instanciate a session"""
        return self._factory(**kwargs)
