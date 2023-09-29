import json
from fastapi import APIRouter, HTTPException, status, Response
import pandas as pd
import boto3
import awswrangler as wr
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from application.utils import models
from application.auth.jwt_handler import signJWT
from application.utils import schemas
from fastapi import HTTPException, status
from passlib.context import CryptContext
from fastapi.responses import JSONResponse
import boto3

import main
import random
import string
from application.auth.jwt_handler import signJWT, decodeJWT
from fastapi import FastAPI, HTTPException, status, File, UploadFile, Depends, Form, Header, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from application.utils import models
import urllib
from application.auth.jwt_bearer import JwtBearer
from botocore.exceptions import ClientError
import awswrangler as wr
import boto3
import json
from fastapi_mail import FastMail, MessageSchema, ConnectionConfig
from fastapi.responses import JSONResponse
import random
import string
import datetime
from fastapi.responses import StreamingResponse
from fastapi_mail import FastMail, MessageSchema, ConnectionConfig
from fastapi.responses import FileResponse
import os
from application.utils.database import SessionLocal, engine
import io
from application.aws_helper.helper import MY_SESSION, S3_CLIENT, SNS_CLIENT
from application.routes.projects import crud
router = APIRouter(tags=["PROJECTS MANAGEMENT"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


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


@router.post("/projects/")
def create_project_metadata(
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


@router.post("/upload/{project_id}")
async def upload_files(files: list[UploadFile], project_id: str,  current_user: dict = Depends(JwtBearer()), db: Session = Depends(get_db)):
    try:
        user_id = current_user['user_id']
        email = current_user['email']

        user = db.query(models.Users).filter(
            (models.Users.user_id == user_id) & (models.Users.email == email)).first()

        tenant = db.query(models.Tenant).filter(
            models.Tenant.tenant_id == user.tenant_id).first()
    except:
        raise HTTPException(
            status_code=403, detail="authentication token expired")
    for file in files:
        try:
            # Generate a unique S3 object key using the file's original name
            object_key = f"project_{project_id}/raw/{file.filename}"

            # Upload the file to S3
            S3_CLIENT.upload_fileobj(
                file.file, tenant.company_name, object_key)

            # Optionally, you can set permissions on the uploaded object
            # s3.put_object_acl(ACL='public-read', Bucket=S3_BUCKET_NAME, Key=object_key)

        except Exception as e:
            return {"message": str(e)}

    return {"message": "Files uploaded successfully"}
