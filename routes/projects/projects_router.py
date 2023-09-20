import json
import schemas
from fastapi import APIRouter, HTTPException, status, Response
import pandas as pd
import boto3
import awswrangler as wr
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
import models
from auth.jwt_handler import signJWT
import schemas
from fastapi import HTTPException, status
from passlib.context import CryptContext
from fastapi.responses import JSONResponse
import boto3

import main
import random
import string
from auth.jwt_handler import signJWT, decodeJWT
from fastapi import FastAPI, HTTPException, status, File, UploadFile, Depends, Form, Header, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import models
import schemas
import urllib
from auth.jwt_bearer import JwtBearer
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
from database import SessionLocal, engine
import io
from aws_helper.helper import MY_SESSION, S3_CLIENT, SNS_CLIENT
from routes.projects import crud
router = APIRouter(tags=["PROJECTS MANAGEMENT"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/projects/{token}")
def all_projects(
    token: str, db: Session = Depends(get_db), current_user: dict = Depends(JwtBearer())
):
    try:
        payload = decodeJWT(token)
        user = (
            db.query(models.Users)
            .filter(models.Users.user_id == payload["user_id"])
            .first()
        )
        # projects = crud.get_user_project(db=db,user_id=payload['user_id'])
        tenant_id = user.tenant_id

        projects = crud.get_projects(db, str(tenant_id))
        return projects
    except:
        return {"response": "token expired"}


@router.get("/projects/user/{token}")
async def read_user_projects(
    token: str, db: Session = Depends(get_db), current_user: dict = Depends(JwtBearer())
):
    try:
        payload = decodeJWT(token)

        projects = crud.get_user_project(db=db, user_id=payload["user_id"])
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
    except ClientError as e:
        print(e)
    except Exception as e:
        response["message"] = e
        return response
    return response
