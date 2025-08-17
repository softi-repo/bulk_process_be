from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    String,
    JSON
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()



class IEBatchRequestLog(Base):
    __tablename__ = 'ie_batch_request_log'

    id = Column(Integer, primary_key=True)
    client_ref_id = Column(String)
    request_id = Column(String, nullable=True)
    pan_list = Column(String, nullable=True)
    cid = Column(Integer)
    env = Column(String)
    input_s3_url = Column(String)
    output_s3_url = Column(String, nullable=True)

    error_message = Column(String, nullable=True)
    total_count = Column(Integer)
    current_statistics = Column(JSON, nullable=True)

    status = Column(String, nullable=True)
    created_on = Column(DateTime, default=datetime.now, nullable=False)
    updated_on = Column(DateTime, default=datetime.now,  nullable=False)

    def __repr__(self):
        return f"<IEBatchRequestLog(Ent={self.cid}, id={self.id})>"
