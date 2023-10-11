import base64
from datetime import datetime
from io import BytesIO

import boto3
import qrcode
from decouple import config
from fastapi import HTTPException, status
from fastapi_mail import ConnectionConfig, FastMail, MessageSchema
from passlib.context import CryptContext
from sqlalchemy.orm import Session

import main as main
from application.auth.jwt_handler import decodeJWT, signJWT, signJWT0
from application.utils import models, schemas, utils


def get_project_by_id(db: Session, project_id: int) -> models.Projects:
    return db.query(models.Projects).get(project_id)


def get_projects_by_tenant_id(db: Session, tenant_id: int) -> list[models.Projects]:
    return (
        db.query(models.Projects).filter(models.Projects.tenant_id == tenant_id).all()
    )


def get_project_by_user_id(db: Session, user_id: int) -> list[models.Projects]:
    return db.query(models.Projects).filter(models.Projects.user_id == user_id).all()


def create_projects(
    user_id: int, tenant_id: str, db: Session, project: schemas.ProjectCreate
) -> models.Projects:
    db_project = models.Projects(
        project_name=project.project_name,
        description=project.description,
        user_id=user_id,
        tenant_id=tenant_id,
        project_status=schemas.ProjectStatus.PENDING,
        start_date=project.start_date,
        months_to_forecast=project.months_to_forecast,
        imtt=project.imtt,
    )

    db.add(db_project)
    db.commit()
    db.refresh(db_project)

    return db_project


def addAssumptionsMetadata(
    project_id: str, input_filename: str, input_object_key, db: Session
):
    try:
        assumptions = models.Assumptionsfiles(
            project_id=project_id,
            input_filename=input_filename,
            input_object_key=input_object_key,
        )

        db.add(assumptions)
        db.commit()
        db.refresh(assumptions)
        print(assumptions)

    except:
        return {"statusCode": status.HTTP_403_FORBIDDEN}

    return status.HTTP_200_OK


def update_project_by_id(
    project_id: str, edit_project: schemas.ProjectUpdate, db: Session
) -> models.Projects:
    project = get_project_by_id(db=db, project_id=project_id)

    project.project_name = edit_project.project_name
    project.description = edit_project.description
    project.start_date = edit_project.start_date
    project.imtt = edit_project.imtt
    db.commit()
    return project


def update_project_status(project_id: str, status: str, db: Session) -> models.Projects:
    project = get_project_by_id(db=db, project_id=project_id)
    project.project_status = status
    db.commit()
    return project


def delete_project_by_id(db: Session, project_id: str) -> models.Projects:
    project = get_project_by_id(db=db, project_id=project_id)
    db.delete(project)
    db.commit()
    return project
