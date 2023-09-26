from datetime import datetime
import enum
from sqlalchemy import Column, Integer
from sqlalchemy.types import Enum as SQLAlchemyEnum
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from database import Base


class Tenant(Base):
    __tablename__ = "tenants"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)

    physical_address = Column(String)
    alt_email = Column(String)
    joined_at = Column(String, default=datetime.now)
    updated_at = Column(String, default=datetime.now)

    admin_id = Column(Integer, ForeignKey("users.id"))
    users = relationship("Users", back_populates="tenant")

    def __str__(self):
        return self.name


class UserRole(enum.Enum):
    SUPER_ADMIN = "SUPER_ADMIN"
    ADMIN = "ADMIN"
    USER = "USER"


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True)
    first_name = Column(String)
    last_name = Column(String)
    is_active = Column(Boolean, default=False)
    role = Column(SQLAlchemyEnum(UserRole), default=UserRole.USER)
    password = Column(String)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(String, default=datetime.now)
    permission = Column(String)

    tenant_id = Column(Integer, ForeignKey("tenants.id"))
    projects = relationship("Projects", back_populates="users")

    def __str__(self):
        return self.email


class ProjectStatus(enum.Enum):
    PENDING = 'PENDING'
    IN_PROGRESS = 'IN_PROGRESS'
    COMPLETED = 'COMPLETED'


class Project(Base):
    __tablename__ = "projects"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    description = Column(String)
    start_date = Column(String)
    status = Column(SQLAlchemyEnum(ProjectStatus),
                    default=ProjectStatus.PENDING)

    created_at = Column(String, default=datetime.now)
    updated_at = Column(String, default=datetime.now)

    id = Column(Integer, ForeignKey("users.id"))
    users = relationship("Users", back_populates="projects")

    def __str__(self):
        return self.name


class AuditTrail(Base):
    __tablename__ = "audit_trails"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.now)
    id = Column(Integer, ForeignKey("users.id"))
    action = Column(String)
    details = Column(String, nullable=True)

    def __str__(self):
        return self.action


class Assumption(Base):
    __tablename__ = "assumptions"

    id = Column(Integer, primary_key=True, index=True)
    assumptions_url = Column(String)
    results_url = Column(String)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)

    project_id = Column(Integer, ForeignKey("project.id"))

    def __str__(self):
        return self.project_id
