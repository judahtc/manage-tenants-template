import datetime
import io
import json
import os
import random
import string
import urllib
from datetime import datetime, timedelta
from multiprocessing.dummy.connection import Listener
from typing import List
from xmlrpc.server import list_public_methods

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

import main as main
from application.auth.jwt_bearer import JwtBearer
from application.auth.jwt_handler import decodeJWT, signJWT
from application.aws_helper.helper import MY_SESSION, S3_CLIENT, SNS_CLIENT
from application.routes.projects import crud
from application.utils import models, schemas
from application.utils.database import SessionLocal, engine, get_db

router = APIRouter(tags=["PROJECTS MANAGEMENT"])


@router.post("/projects/{project_id}/assumptions")
def add_assumptions(
    project_id: str,
    assumptions: schemas.Assumptions,
    db: Session = Depends(get_db),
    current_user: dict = Depends(JwtBearer()),
):
    assumption = crud.add_Assumptions(
        project_id=project_id, db=db, project_input=assumptions
    )
    if assumptions:
        return assumption
    else:
        raise HTTPException(status_code=assumptions.statusCode)


# @router.get("/projects/{project_id}/assumptions")
# def read_assumptions(
#     project_id: int,
#     db: Session = Depends(get_db),
#     current_user: dict = Depends(JwtBearer()),
# ):
#     db_assumptions = crud.get_assumptions(db, project_id=project_id)
#     if db_assumptions is None:
#         raise HTTPException(status_code=404, detail="Project not found")
#     return db_assumptions


@router.post("/upload/{project_id}/assumptions")
async def upload_files(
    files: list[UploadFile],
    project_id: str,
    current_user: dict = Depends(JwtBearer()),
    db: Session = Depends(get_db),
):
    now = datetime.datetime.now()
    try:
        user_id = current_user["user_id"]
        email = current_user["email"]

        user = (
            db.query(models.Users)
            .filter((models.Users.user_id == user_id) & (models.Users.email == email))
            .first()
        )

        tenant = (
            db.query(models.Tenant)
            .filter(models.Tenant.tenant_id == user.tenant_id)
            .first()
        )
    except:
        raise HTTPException(status_code=403, detail="authentication token expired")
    for file in files:
        try:
            # Generate a unique S3 object key using the file's original name
            filename = now.strftime("%Y%m%d%H%M%S") + "_" + file.filename
            object_key = f"project_{project_id}/assumptions/{filename}"

            # Upload the file to S3
            S3_CLIENT.upload_fileobj(file.file, tenant.company_name, object_key)
            # Optionally, you can set permissions on the uploaded object

            # s3.put_object_acl(ACL='public-read', Bucket=S3_BUCKET_NAME, Key=object_key)

        except Exception as e:
            return {"message": str(e)}
    add_meta_data = crud.addAssumptionsMetadata(
        project_id=project_id,
        input_filename=filename,
        input_object_key=object_key,
        db=db,
    )

    if add_meta_data == status.HTTP_200_OK:
        return {"message": "Files uploaded successfully"}
    else:
        raise HTTPException(status_code=add_meta_data)


@router.get("/{project_id}/assumptions")
def list_objects_in_partition(
    project_id: str,
    current_user: dict = Depends(JwtBearer()),
    db: Session = Depends(get_db),
):
    partition_prefix = f"project_{project_id}/assumptions/"

    user_id = current_user["user_id"]
    email = current_user["email"]

    user = db.query(models.Users).filter(models.Users.user_id == user_id).first()
    # projects = crud.get_user_project(db=db,user_id=payload['user_id'])

    tenant_id = user.tenant_id
    tenant = (
        db.query(models.Tenant).filter(models.Tenant.tenant_id == tenant_id).first()
    )

    response = S3_CLIENT.list_objects_v2(
        Bucket=tenant.company_name, Prefix=partition_prefix
    )

    object_keys = []
    if "Contents" in response:
        for obj in response["Contents"]:
            object_key = obj["Key"]
            if object_key != partition_prefix:
                file_name = object_key.split("/")[-1]
                object_keys.append(file_name)

    return object_keys


@router.get("/{project_id}/assumptions/latest/")
def latest_assumption(
    project_id: str,
    current_user: dict = Depends(JwtBearer()),
    db: Session = Depends(get_db),
):
    user_id = current_user["user_id"]
    email = current_user["email"]

    user = db.query(models.Users).filter(models.Users.user_id == user_id).first()
    # projects = crud.get_user_project(db=db,user_id=payload['user_id'])

    tenant_id = user.tenant_id
    tenant = (
        db.query(models.Tenant).filter(models.Tenant.tenant_id == tenant_id).first()
    )

    file_list = list_objects_in_partition(
        bucket_name=tenant.company_name, project_id=project_id
    )
    # Convert the file names to datetime objects
    date_format = "%Y%m%d%H%M%S"
    dates = [
        datetime.datetime.strptime(file_name.split("_")[0], date_format)
        for file_name in file_list
    ]

    # Find the maximum datetime object
    max_date = max(dates)

    # Find the index of the maximum datetime object
    max_index = dates.index(max_date)

    # Retrieve the most recent file name
    most_recent_file = file_list[max_index]
    most_recent_files = json.loads(most_recent_file)
    return most_recent_files


@router.get("/{project_id}/assumptions/{file_name}")
def read_s3_file(
    file_name: str,
    project_id: str,
    current_user: dict = Depends(JwtBearer()),
    db: Session = Depends(get_db),
):
    user_id = current_user["user_id"]
    email = current_user["email"]

    user = db.query(models.Users).filter(models.Users.user_id == user_id).first()
    # projects = crud.get_user_project(db=db,user_id=payload['user_id'])

    tenant_id = user.tenant_id
    tenant = (
        db.query(models.Tenant).filter(models.Tenant.tenant_id == tenant_id).first()
    )
    object_key = f"project_{project_id}/assumptions/{file_name}"
    obj = S3_CLIENT.get_object(Bucket=tenant.company_name, Key=object_key)
    file_content = obj["Body"].read().decode("utf-8")

    # Use StringIO to create a string buffer
    csv_buffer = io.StringIO(file_content)

    df = pd.read_csv(csv_buffer)
    json_data = df.to_json(orient="records")
    data = json.loads(json_data)
    return data
