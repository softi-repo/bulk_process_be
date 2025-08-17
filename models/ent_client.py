from sqlalchemy import (
    Column,
    Integer,
    String
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class ClientService(Base):

    __tablename__ = "client_service"

    id = Column(Integer, primary_key=True)
    cid = Column(Integer, nullable=False)
    client_id = Column(String, nullable=False, unique=True)
    client_secret = Column(String, nullable=False)
    service_id = Column(String, nullable=False)
    status = Column(Integer)

    def __repr__(self):
        return f"<CLIENT_SERVICES(EID={self.cid})>"
