from datetime import datetime

from sqlalchemy.orm import declarative_base, Mapped
from sqlalchemy import Column, String, DateTime

Base = declarative_base()


class RespoitoriesORM(Base):
    id: Mapped[str] = Column(String, primary_key=True)
    date_created: Mapped[datetime] = Column(
        DateTime, nullable=False, default=datetime.now()
    )
