import json
from multiprocessing.dummy.connection import Listener
from xmlrpc.server import list_public_methods
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
from typing import List
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
import datetime

router = APIRouter(tags=["PROJECTS MANAGEMENT"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/projects/{project_id}/assumptions")
def add_assumptions(
    project_id: str,
    assumptions: schemas.Assumptions,
    db: Session = Depends(get_db),
    current_user: dict = Depends(JwtBearer()),
):

    assumption = crud.add_Assumptions(
        project_id=project_id, db=db, project_input=assumptions)
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
async def upload_files(files: list[UploadFile], project_id: str,  current_user: dict = Depends(JwtBearer()), db: Session = Depends(get_db)):
    now = datetime.datetime.now()
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
            filename = now.strftime('%Y%m%d%H%M%S')+'_'+file.filename
            object_key = f"project_{project_id}/assumptions/{filename}"

            # Upload the file to S3
            S3_CLIENT.upload_fileobj(
                file.file, tenant.company_name, object_key)
            # Optionally, you can set permissions on the uploaded object

            # s3.put_object_acl(ACL='public-read', Bucket=S3_BUCKET_NAME, Key=object_key)

        except Exception as e:
            return {"message": str(e)}
    add_meta_data = crud.addAssumptionsMetadata(
        project_id=project_id, input_filename=filename, input_object_key=object_key, db=db)

    if add_meta_data == status.HTTP_200_OK:
        return {"message": "Files uploaded successfully"}
    else:
        raise HTTPException(status_code=add_meta_data)
