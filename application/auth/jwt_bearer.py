import os

import jwt
from decouple import config
from fastapi import HTTPException, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import PyJWTError
from passlib.context import CryptContext
from application.routes.users import crud as users_crud
from application.utils import crud

JWT_SECRET = config("secret")
JWT_ALGORITHM = config("algorithm")

# CryptContext for password hashing and verification
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class JwtBearer(HTTPBearer):
    def __init__(self, auto_error: bool = True):
        super().__init__(auto_error=auto_error)

    async def __call__(self, request: Request):
        authorization: str = request.headers.get("Authorization")
        if authorization and "bearer" in authorization.lower():
            scheme, credentials = authorization.split()

            if scheme.lower() == "bearer":
                try:
                    payload = jwt.decode(
                        credentials, JWT_SECRET, algorithms=[JWT_ALGORITHM]
                    )

                    return payload
                except PyJWTError:
                    raise HTTPException(
                        status_code=401, detail="Invalid authentication token"
                    )
        raise HTTPException(status_code=401, detail="Invalid authentication scheme")
