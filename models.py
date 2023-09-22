from datetime import datetime
import enum
from sqlalchemy import Column, Integer
from sqlalchemy.types import Enum as SQLAlchemyEnum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from database import Base


class ProjectStatusEnum(enum.Enum):
    PENDING = 'PENDING'
    IN_PROGRESS = 'IN_PROGRESS'
    COMPLETED = 'COMPLETED'


class Tenant(Base):
    __tablename__ = "tenants"

    tenant_id = Column(Integer, primary_key=True, index=True)  # Auto generated
    admin_email = Column(String)  # requires input
    password = Column(String)  # requires input
    first_name = Column(String)  # requires input
    last_name = Column(String)  # requires input
    company_name = Column(String)  # requires input
    physical_address = Column(String)  # requires input
    phone_number = Column(String)  # requires input
    created_at = Column(String, default=datetime.now)  # auto captured
    updated_at = Column(String, default=datetime.now)  # requires input
    users = relationship("Users", back_populates="tenant")
    # projects = relationship("Projects", back_populates="tenant")


class Users(Base):
    __tablename__ = "users"

    user_id = Column(Integer, primary_key=True, index=True)  # auto generated
    email = Column(String, unique=True)  # requires input
    tenant_id = Column(Integer, ForeignKey(
        "tenants.tenant_id"))  # stored in a session
    first_name = Column(String)  # requires input
    last_name = Column(String)  # requires input
    hashed_password = Column(String)  # requires input
    is_active = Column(Boolean, default=False)
    is_admin = Column(Boolean, default=False)
    is_creator = Column(Boolean, default=True)
    is_viewer = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now)  # auto generated
    updated_at = Column(String, default=datetime.now)  # requires input
    phone_number = Column(String, default=0)
    work_address = Column(String)
    secret_key = Column(String)

    tenant = relationship("Tenant", back_populates="users")
    projects = relationship("Projects", back_populates="users")


class AdminUser(Base):
    __tablename__ = "admin_users"

    user_id = Column(Integer, primary_key=True, index=True)  # auto generated
    email = Column(String, unique=True)  # requires input
    # tenant_id = Column(Integer, ForeignKey("tenants.tenant_id")) #stored in a session
    first_name = Column(String)  # requires input
    last_name = Column(String)  # requires input
    hashed_password = Column(String)  # requires input
    is_active = Column(Boolean, default=True)
    is_admin = Column(Boolean, default=False)
    is_creator = Column(Boolean, default=True)
    is_viewer = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now)  # auto generated
    updated_at = Column(String, default=datetime.now)  # requires input
    phone_number = Column(String, default=0)
    work_address = Column(String)

    # tenant = relationship("Tenant", back_populates="users")
    # projects = relationship("Projects", back_populates="users")


class Projects(Base):
    __tablename__ = "projects"

    project_id = Column(Integer, primary_key=True, index=True)
    project_name = Column(String)
    created_at = Column(String, default=datetime.now)
    description = Column(String)
    updated_at = Column(String, default=datetime.now)
    start_date = Column(String)
    tenant_id = Column(String)
    project_status = Column(SQLAlchemyEnum(ProjectStatusEnum), nullable=False)
    user_id = Column(Integer, ForeignKey("users.user_id"))
    users = relationship("Users", back_populates="projects")


class AuditTrail(Base):
    __tablename__ = "audit_trail"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.now)
    email_address = Column(String)
    action = Column(String)
    details = Column(String, nullable=True)
    tenant_id = Column(String)


class Assumptions(Base):
    __tablename__ = "assumptions"
    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)
    project_id = Column(String)
    interest_calculation_method = Column(String)
    depreciation_method = Column(String)
    average_loan_term = Column(String)
    inflation_rate = Column(String)
    number_of_months_to_focast = Column(String)
    administration_fee = Column(String)
    isActive = Column(Boolean, default=True)
