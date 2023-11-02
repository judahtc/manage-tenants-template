from datetime import datetime
from enum import Enum

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.types import Enum as SQLAlchemyEnum

from application.utils import schemas

from .database import Base


class Tenant(Base):
    __tablename__ = "tenants"

    tenant_id = Column(Integer, primary_key=True, index=True)  # Auto generated
    admin_email = Column(String)  # requires input
    first_name = Column(String)  # requires input
    last_name = Column(String)  # requires input
    company_name = Column(String)  # requires input
    physical_address = Column(String)  # requires input
    phone_number = Column(String)  # requires input
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())  # auto captured
    updated_at = Column(
        DateTime, default=func.now(), onupdate=func.now()
    )  # requires input

    users = relationship("Users", back_populates="tenant")
    projects = relationship("Projects", back_populates="tenant")


class Users(Base):
    __tablename__ = "users"

    user_id = Column(Integer, primary_key=True, index=True)  # auto generated
    email = Column(String, unique=True)  # requires input
    tenant_id = Column(Integer, ForeignKey("tenants.tenant_id"))  # stored in a session
    first_name = Column(String)  # requires input
    last_name = Column(String)  # requires input
    hashed_password = Column(String)  # requires input
    phone_number = Column(String)  # required input
    role = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    secret_key = Column(String)
    created_at = Column(DateTime, default=func.now())  # auto captured
    updated_at = Column(
        DateTime, default=func.now(), onupdate=func.now()
    )  # requires input

    tenant = relationship("Tenant", back_populates="users")
    projects = relationship("Projects", back_populates="users")


class Projects(Base):
    __tablename__ = "projects"

    tenant_id = Column(Integer, ForeignKey("tenants.tenant_id"))
    user_id = Column(Integer, ForeignKey("users.user_id"))
    project_id = Column(Integer, primary_key=True, index=True)
    project_name = Column(String)
    description = Column(String)
    start_date = Column(Date)
    imtt = Column(Float)
    months_to_forecast = Column(Integer)
    project_status = Column(
        String, nullable=False, default=schemas.ProjectStatus.PENDING
    )
    created_at = Column(DateTime, default=func.now())  # auto captured
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    users = relationship("Users", back_populates="projects")
    tenant = relationship("Tenant", back_populates="projects")


class AuditTrail(Base):
    __tablename__ = "audit_trail"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.now)
    email_address = Column(String)
    action = Column(String)
    details = Column(String, nullable=True)
    tenant_id = Column(Integer)
