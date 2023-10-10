import datetime
import io
import json
import os
import random
import string
import urllib
from datetime import datetime, timedelta
from typing import List

import awswrangler as wr
import boto3
import pandas as pd
from botocore.exceptions import ClientError
from fastapi import (
    APIRouter,
    Depends,
    FastAPI,
    File,
    Form,
    Header,
    HTTPException,
    Request,
    Response,
    UploadFile,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi_mail import ConnectionConfig, FastMail, MessageSchema
from passlib.context import CryptContext
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker

import application.main as main
from application.auth.jwt_bearer import JwtBearer
from application.auth.jwt_handler import decodeJWT, signJWT
from application.aws_helper.helper import MY_SESSION, S3_CLIENT, SNS_CLIENT
from application.modeling import helper
from application.routes.projects import crud
from application.utils import models, schemas
from application.utils.database import SessionLocal, engine

router = APIRouter(tags=["PROJECTS MANAGEMENT"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/projects/")
def create_project(
    project: schemas.ProjectsCreate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(JwtBearer()),
):
    user_id = current_user['user_id']
    email = current_user['email']

    user = db.query(models.Users).filter(
        (models.Users.user_id == user_id) & (models.Users.email == email)).first()

    tenant = db.query(models.Tenant).filter(
        models.Tenant.tenant_id == user.tenant_id).first()

    # Create the projet on the database.
    project = crud.create_projects(
        user_id=user_id,
        tenant_id=tenant.tenant_id,
        db=db,
        project=project
    )

    # Create the project s3 bucket.
    if project:
        bucket = create_project(project.project_id, tenant.company_name)
    return project


# @router.post("/upload/{project_id}")
# def upload_files(
#     project_id: int, files: List[UploadFile] = File(...), current_user: dict = Depends(JwtBearer()), db: Session = Depends(get_db)
# ):

#     user_id = current_user['user_id']
#     email = current_user['email']

#     user = db.query(models.Users).filter(
#         (models.Users.user_id == user_id) & (models.Users.email == email)).first()

#     tenant = db.query(models.Tenant).filter(
#         models.Tenant.tenant_id == user.tenant_id).first()
#     tenant_name = tenant.company_name
#     return helper.upload_multiple_files(
#         project_id=project_id,
#         tenant_name=tenant_name,
#         my_session=MY_SESSION,
#         files=files,
#     )


@router.get("/projects")
def all_projects(
    db: Session = Depends(get_db), current_user: dict = Depends(JwtBearer())
):
    try:
        user_id = current_user['user_id']
        email = current_user['email']

        user = (
            db.query(models.Users)
            .filter(models.Users.user_id == user_id)
            .first()
        )
        # projects = crud.get_user_project(db=db,user_id=payload['user_id'])
        tenant_id = user.tenant_id

        projects = crud.get_projects(db, str(tenant_id))
        return projects
    except:
        return {"response": "token expired"}


@router.get("/projects/user")
async def read_user_projects(
    db: Session = Depends(get_db), current_user: dict = Depends(JwtBearer())
):
    try:
        user_id = current_user['user_id']
        projects = crud.get_user_project(db=db, user_id=user_id)
        return projects
    except:
        return {"response": "token expired"}


@router.get("/projects/{project_id}", response_model=schemas.Projects)
def read_project(
    project_id: int,
    db: Session = Depends(get_db),
    current_user: dict = Depends(JwtBearer()),
):
    db_project = crud.get_project(db, project_id=project_id)
    if db_project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return db_project


@router.put("/projects/{project_id}")
def update_project(
    project_id: str,
    edit_project: schemas.ProjectUpdate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(JwtBearer()),
):
    return crud.update_project(project_id, edit_project=edit_project, db=db)


@router.delete("/projects/{project_id}")
async def delete_project(project_id: str, db: Session = Depends(get_db)):
    try:
        return crud.delete_project(db=db, project_id=project_id)

    except:
        return {"response": "project does not exist ", "statusCode": status.HTTP_404_NOT_FOUND}


# @router.post("/create_project/{tenant_name}/{project_id}")
def create_project(
    project_id: int, tenant_name: str

):
    response = {}
    try:
        if f"s3://{tenant_name}/project_{project_id}/" in wr.s3.list_directories(
            f"s3://{tenant_name}/", boto3_session=MY_SESSION
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"project with id {project_id} already exists",
            )
    except ClientError as e:
        print(e)

    try:
        S3_CLIENT.put_object(
            Bucket=f"{tenant_name}", Key=f"project_{project_id}/raw/", Body=""
        )
        S3_CLIENT.put_object(
            Bucket=f"{tenant_name}",
            Key=f"project_{project_id}/needs-attention/",
            Body="",
        )
        S3_CLIENT.put_object(
            Bucket=f"{tenant_name}", Key=f"project_{project_id}/modeling/", Body=""
        )
        S3_CLIENT.put_object(
            Bucket=f"{tenant_name}", Key=f"project_{project_id}/results/", Body=""
        )
        response["message"] = f"Project with id {project_id} created successfully"

    except Exception as e:
        response["message"] = e
        return response
    return response


# @router.post("/upload/{project_id}")
# async def upload_files(files: list[UploadFile], project_id: str,  current_user: dict = Depends(JwtBearer()), db: Session = Depends(get_db)):
#     try:
#         user_id = current_user['user_id']
#         email = current_user['email']

#         user = db.query(models.Users).filter(
#             (models.Users.user_id == user_id) & (models.Users.email == email)).first()

#         tenant = db.query(models.Tenant).filter(
#             models.Tenant.tenant_id == user.tenant_id).first()
#     except:
#         raise HTTPException(
#             status_code=403, detail="authentication token expired")
#     for file in files:
#         try:
#             # Generate a unique S3 object key using the file's original name
#             object_key = f"project_{project_id}/raw/{file.filename}"

#             # Upload the file to S3
#             S3_CLIENT.upload_fileobj(
#                 file.file, tenant.company_name, object_key)

#             # Optionally, you can set permissions on the uploaded object
#             # s3.put_object_acl(ACL='public-read', Bucket=S3_BUCKET_NAME, Key=object_key)

#         except Exception as e:
#             return {"message": str(e)}

#     return {"message": "Files uploaded successfully"}
