import os
from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel
from sqlalchemy.orm import Session

from application.routes.users import crud as users_crud
from application.utils import models, schemas
from application.utils.database import get_db

SECRET_KEY = os.getenv("secret")
ALGORITHM = os.getenv("algorithm")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class TokenData(BaseModel):
    username: Optional[str] = None


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    return pwd_context.hash(password)


def authenticate_user(
    db: Session, username: str, password: str, expires_delta: timedelta | None = None
):
    user = users_crud.get_user_by_email(db, username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False

    access_token = create_access_token(
        data={"email": user.email}, expires_delta=expires_delta
    )

    user.access_token = access_token
    user.token_type = "bearer"

    return user


def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=5)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(
    token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)
):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("email")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    user = users_crud.get_user_by_email(db=db, email=token_data.username)
    if user is None:
        raise credentials_exception
    return user


async def get_current_active_user(
    current_user: models.Users = Depends(get_current_user),
) -> schemas.UserLoginResponse:
    if not current_user.tenant.is_active:
        raise HTTPException(
            detail="Tenant is Inactive", status_code=status.HTTP_401_UNAUTHORIZED
        )

    if not current_user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Inactive user"
        )
    return current_user


def _decode_token(token: str, db: Session):
    payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    username: str = payload.get("email")
    if username is None:
        pass

    token_data = TokenData(username=username)

    return users_crud.get_user_by_email(db=db, email=token_data.username)
