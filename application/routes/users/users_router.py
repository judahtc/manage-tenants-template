import datetime
import io
import json
import os
import random
import string
import urllib
from datetime import datetime, timedelta
from typing import List, Optional, Union

import awswrangler as wr
import boto3
import pandas as pd
import pyotp
import qrcode
from botocore.exceptions import ClientError
from decouple import config
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
from application.auth.security import get_current_active_user
from application.routes.users import crud, emails
from application.utils import models, schemas, utils
from application.utils.database import SessionLocal, engine, get_db

router = APIRouter(
    tags=["USER MANAGEMENT"], dependencies=[Depends(get_current_active_user)]
)


@router.post("/users/", response_model=schemas.UserResponse)
async def create_user(
    user: schemas.UsersBaseCreate,
    db: Session = Depends(get_db),
    current_user: schemas.UserLoginResponse = Depends(get_current_active_user),
):
    if current_user.role not in [schemas.UserRole.ADMIN, schemas.UserRole.SUPERADMIN]:
        raise HTTPException(
            detail="You're not authorized to perform this action",
            status_code=status.HTTP_401_UNAUTHORIZED,
        )

    if (
        current_user.role == schemas.UserRole.ADMIN
        and user.role == schemas.UserRole.SUPERADMIN
    ):
        raise HTTPException(
            detail="You're not authorized to perform this action",
            status_code=status.HTTP_401_UNAUTHORIZED,
        )

    random_password = utils.generate_random_password()
    secret_key = pyotp.random_base32()

    uri = pyotp.totp.TOTP(secret_key).provisioning_uri(
        name="Claxon", issuer_name="CBS Budgetting"
    )

    qrcode_image = crud.create_base64_qrcode_image(uri)

    created_user = crud.create_user(
        db=db,
        user=user,
        password=random_password,
        secret_key=secret_key,
        admin=current_user,
    )

    emails.send_email(
        recipient=user.email,
        qrcode_image=qrcode_image,
        password=random_password,
    )

    return created_user


@router.get("/users/", response_model=list[schemas.UserResponse])
def get_users_by_tenant_id(
    db: Session = Depends(get_db),
    current_user: schemas.UserLoginResponse = Depends(get_current_active_user),
):
    users = crud.get_users(db, tenant_id=current_user.tenant_id)
    return users


@router.get("/users/{user_id}", response_model=schemas.UserResponse)
async def get_user_by_id(
    user_id: int,
    db: Session = Depends(get_db),
):
    user = crud.get_user_by_id(db=db, user_id=user_id)
    if user is None:
        raise HTTPException(
            detail="User does not exist", status_code=status.HTTP_404_NOT_FOUND
        )

    return user


@router.patch("/users/{user_id}/toggle-active")
def toggle_users_active(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: schemas.UserLoginResponse = Depends(get_current_active_user),
):
    if current_user.role not in [schemas.UserRole.ADMIN, schemas.UserRole.SUPERADMIN]:
        raise HTTPException(
            detail="You're not authorized to perform this action",
            status_code=status.HTTP_401_UNAUTHORIZED,
        )

    if current_user.user_id == user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="You can't deactive yourself"
        )

    user = crud.get_user_by_id(db=db, user_id=user_id)
    user = crud.get_user_by_id(db=db, user_id=user_id)
    user.is_active = not user.is_active

    db.commit()
    db.refresh(user)

    return user


@router.delete("/users/{user_id}")
async def delete_user_by_id(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: schemas.UserLoginResponse = Depends(get_current_active_user),
):
    if current_user.role not in [schemas.UserRole.ADMIN, schemas.UserRole.SUPERADMIN]:
        raise HTTPException(
            detail="You're not authorized to perform this action",
            status_code=status.HTTP_401_UNAUTHORIZED,
        )

    if current_user.user_id == user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="You can't delete yourself"
        )

    user = crud.get_user_by_id(db=db, user_id=user_id)

    if user is None:
        raise HTTPException(
            detail="User does not exist", status_code=status.HTTP_404_NOT_FOUND
        )

    if user.role in [schemas.UserRole.ADMIN, schemas.UserRole.SUPERADMIN]:
        raise HTTPException(detail="Cannot delete an admin user")

    user = crud.get_user_by_id(db=db, user_id=user_id)
    crud.delete_by_email(db=db, email=user.email)

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put("/users/{user_id}")
def update_user_by_id(
    user_id: int,
    edit_user: schemas.UserUpdate,
    db: Session = Depends(get_db),
    current_user: schemas.UserLoginResponse = Depends(get_current_active_user),
):
    user = crud.get_user_by_id(db=db, user_id=user_id)
    return crud.update_by_email(user.email, edit_user=edit_user, db=db)
