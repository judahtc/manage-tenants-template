from typing import Union
from routes.users import users_router
from routes.tenants import tenants_router
from routes.projects import projects_router
from routes.projects import assumptions
from fastapi import FastAPI
import datetime
import json
import os
import random
import string
from typing import List
import google_auth
import awswrangler as wr
from botocore.exceptions import ClientError
from decouple import config
from fastapi import Depends, FastAPI, File, HTTPException, Request, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import pyotp
import qrcode
import crud
import models
import schemas
from auth.jwt_bearer import JwtBearer
from auth.jwt_handler import decodeJWT, signJWT
from database import SessionLocal, engine


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


app = FastAPI()

models.Base.metadata.create_all(bind=engine)

origins = ["*"]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


@app.get("/")
def read_root():
    return {"Hello": "World, l am CICD 2,the first one didnt work,pipeline failed on the second one but the code worked"}


@app.post("/user/login")
def login(user: schemas.UserLoginSchema, db: Session = Depends(get_db)):
    user = crud.check_user(db=db, user=user)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email and password"
        )

    return user


app.include_router(users_router.router)
app.include_router(tenants_router.router)
app.include_router(projects_router.router)
app.include_router(assumptions.router)
