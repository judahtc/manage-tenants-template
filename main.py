from datetime import datetime, timedelta

import pyotp
from fastapi import Depends, FastAPI, File, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from mangum import Mangum
from pydantic import EmailStr
from sqlalchemy.orm import Session

from application.auth import security
from application.auth.security import _decode_token, get_current_active_user
from application.routes import final_calculations, intermediate_calculations
from application.routes.projects import projects_router
from application.routes.tenants import tenants_router
from application.routes.users import users_router
from application.utils import database, models, schemas, utils
from application.utils.database import SessionLocal, engine, get_db

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


@app.middleware("http")
async def audit_middleware(request: Request, call_next):
    response = await call_next(request)

    try:
        auth = request.headers["Authorization"]
        _, token = auth.split()
        db = SessionLocal()
        user = _decode_token(token=token, db=db)

        variable = utils.safe_dict_lookup(
            key1="user_id", key2="project_id", my_dict=request.path_params
        )

        endpoint_details = utils.get_endpoint_details(variable=variable)

        audit_trail_entry = models.AuditTrail(
            email_address=user.email,
            action=request.url.path,
            details=endpoint_details.get(request.url.path),
            tenant_id=user.tenant_id,
        )

        db.add(audit_trail_entry)
        db.commit()
        db.refresh(audit_trail_entry)
    except:
        pass

    return response


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


@app.get("/login-verification", response_model=schemas.UserLoginResponse)
async def login_verification(
    otp_code: int,
    current_user: models.Users = Depends(get_current_active_user),
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


@app.post("/reset-password")
def reset_password(
    payload: schemas.ResetPassword,
    current_user: models.Users = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    hashed_password = security.get_password_hash(payload.new_password)
    current_user.hashed_password = hashed_password
    db.commit()
    return {"detail": f"Password for {current_user.email} changed successfully "}


@app.post("/add-audit-trail")
def add_audit_trail(
    audit_trail: schemas.AuditTrailBase,
    current_user: models.Users = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    audit_trail_entry = models.AuditTrail(
        email_address=current_user.email,
        action=audit_trail.action,
        details=audit_trail.details,
        tenant_id=current_user.tenant_id,
    )

    db.add(audit_trail_entry)
    db.commit()
    db.refresh(audit_trail_entry)
    return audit_trail_entry


@app.post("/extract-audit-trail")
def extract_audit_trail(
    extract_audit_trail: schemas.ExtractAuditTrail,
    current_user: models.Users = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    print(extract_audit_trail.start_date)
    audit_trail_entries = (
        db.query(models.AuditTrail)
        .filter(
            (models.AuditTrail.timestamp >= extract_audit_trail.start_date)
            & (models.AuditTrail.timestamp <= extract_audit_trail.end_date)
            & (models.AuditTrail.tenant_id == current_user.tenant_id)
        )
        .order_by(models.AuditTrail.timestamp.desc())
        .all()
    )

    return audit_trail_entries


app.include_router(tenants_router.router)
app.include_router(users_router.router)
app.include_router(projects_router.router)
app.include_router(intermediate_calculations.router)
app.include_router(final_calculations.router)
