

from datetime import datetime
import qrcode
import base64
from io import BytesIO
from auth.jwt_handler import decodeJWT, signJWT, signJWT0
import google_auth
import schemas
import models
import main
from sqlalchemy.orm import Session
from passlib.context import CryptContext
from fastapi_mail import ConnectionConfig, FastMail, MessageSchema
from fastapi import HTTPException, status
from decouple import config
import boto3


def get_projects(db: Session, tenant_id: str):
    return (
        db.query(models.Projects).filter(
            models.Projects.tenant_id == tenant_id).all()
    )


def get_project(db: Session, project_id: int):
    return (
        db.query(models.Projects)
        .filter(models.Projects.project_id == project_id)
        .first()
    )


def get_user_project(db: Session, user_id: int):
    return db.query(models.Projects).filter(models.Projects.user_id == user_id).all()


def get_projects_with_cb(db: Session, tenant_id: str):
    return (
        db.query(models.Projects).filter((models.Projects.tenant_id == tenant_id) & (
            models.Projects.closing_balances == True) & (models.Projects.closing_balances_lic == True)).all()
    )


# import emails_helper
# from modeling import helper


def create_projects(user_id: int, tenant_id: str, db: Session, project: schemas.ProjectsCreate):

    date_now = datetime.now()
    db_project = models.Projects(
        project_name=project.project_name,
        description=project.description,
        user_id=user_id,
        tenant_id=tenant_id,
        project_status="PENDING",
        start_date=project.start_date,
    )
    db.add(db_project)
    db.commit()
    db.refresh(db_project)
    print(db_project)
    return db_project


def update_project(project_id: str, edit_project: schemas.ProjectUpdate, db: Session):
    # try:

    project = get_project(db=db, project_id=project_id)
    if project is not None:
        project.project_name = edit_project.project_name
        project.updated_at = datetime.now()
        project.description = edit_project.description

        print(datetime.now)

        db.commit()
    else:
        return {"response": "project does not exist ", "statusCode": status.HTTP_404_NOT_FOUND}


def delete_project(db: Session, project_id: str):
    try:
        project = get_project(db=db, project_id=project_id)
        if project is not None:
            db.delete(project)
            db.commit()
            return {"response": "project successfully deleted ", "statusCode": status.HTTP_200_OK}
        else:
            return {"response": "project does not exist ", "statusCode": status.HTTP_404_NOT_FOUND}
    except:
        return {"response": "project does not exist ", "statusCode": status.HTTP_404_NOT_FOUND}
