import io
from datetime import timedelta
from typing import List, Union

import awswrangler as wr
import pyotp
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.security import OAuth2PasswordRequestForm
from mangum import Mangum
from sqlalchemy.orm import Session

from application.auth import security
from application.auth.jwt_bearer import JwtBearer
from application.auth.security import get_current_active_user
from application.aws_helper.helper import MY_SESSION, S3_CLIENT, SNS_CLIENT
from application.modeling import constants, helper
from application.routes import final_calculations, intermediate_calculations
from application.routes.projects import assumptions, projects_router
from application.routes.tenants import tenants_router
from application.routes.users import users_router
from application.utils import crud, database, models, schemas
from application.utils.database import engine, get_db

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
handler = Mangum(app)


@app.get("/")
def root():
    return {"message": "deployment to dev using pipeline"}


@app.get("/health")
def health():
    return {"status": "ok", "another": [1, 2, 3]}


database.Base.metadata.create_all(bind=engine)

origins = ["*"]


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {
        "Hello": "World, l am CICD 2,the first one didnt work,pipeline failed on the second one but the code worked"
    }


@app.post("/v1/login", response_model=schemas.UserLoginResponse)
def login(user: schemas.UserLogin, db: Session = Depends(get_db)):
    user = security.authenticate_user(
        db=db, username=user.email, password=user.password
    )

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
        )
    return user


@app.post("/login", response_model=schemas.UserLoginResponse)
def login(user: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = security.authenticate_user(
        db=db,
        username=user.username,
        password=user.password,
        expires_delta=timedelta(hours=24),
    )

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Inactive User"
        )

    return user


@app.get("/login_verification", response_model=schemas.UserLoginResponse)
async def login_verification(
    otp_code: int,
    current_user: schemas.UserLoginResponse = Depends(get_current_active_user),
):
    otp_verification = pyotp.TOTP(current_user.secret_key).verify(otp_code)

    if not otp_verification:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect otp token",
        )

    access_token = security.create_access_token(
        data={"email": current_user.email},
        expires_delta=timedelta(hours=24),
    )

    current_user.access_token = access_token
    current_user.token_type = "bearer"

    return current_user


app.include_router(tenants_router.router)
app.include_router(users_router.router)
app.include_router(projects_router.router)
app.include_router(intermediate_calculations.router)
app.include_router(final_calculations.router)
