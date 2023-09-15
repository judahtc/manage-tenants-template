from fastapi import HTTPException, Security, FastAPI, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from passlib.context import CryptContext
import jwt
from jwt import PyJWTError

# Secret key to sign and verify JWT tokens
SECRET_KEY = "29dc01ff7affea15363091e8e5c5aa1dbeae1554df46ab7a11af9d2dd2b09eec"

# Algorithm used for JWT encoding and decoding
ALGORITHM = "HS256"

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
                        credentials, SECRET_KEY, algorithms=[ALGORITHM])
                    return payload
                except PyJWTError:
                    raise HTTPException(
                        status_code=401, detail="Invalid authentication token")
        raise HTTPException(
            status_code=401, detail="Invalid authentication scheme")


app = FastAPI()
