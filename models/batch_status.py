from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    Integer,
    func,
    Text,
    String
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class IeBatchRunLog(Base):
    __tablename__ = "ie_individual_run_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    env = Column(Enum('Dev', 'Stage', 'Demo', 'Prod'), default='Prod')
    cid = Column(Integer, nullable=True)
    request_id = Column(String(100), nullable=True)
    batch_request_auto_id = Column(String(100), nullable=True)
    batch_ref_num= Column(String(100), nullable=True)
    client_ref_id = Column(String(45), nullable=True)
    request_body = Column(Text, nullable=True)
    pan = Column(String(10), nullable=True)
    process_id = Column(Integer, default=1)
    http_response_code = Column(Integer, nullable=True)
    retry_count = Column(Integer, nullable=True)
    processing_status = Column(Enum(
        'Open','Inprogress','Completed','Error','Failure','Hold'
    ), default='OPEN')
    response = Column(Text, nullable=True)
    start_time = Column(DateTime, nullable=True)
    tat = Column(Integer, nullable=True)
    created_on = Column(DateTime, server_default=func.now())
    updated_on = Column(DateTime, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<IeBatchRunLog(Ent={self.cid}, id={self.id})>"