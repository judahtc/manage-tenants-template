import pyotp
import qrcode
from auth.jwt_handler import decodeJWT, signJWT, signJWT0


def generate_random_key():
    secret_key = pyotp.random_base32()
    return secret_key


def check_otp(otp: str, secret_key: str, user_id: str):
    try:

        verify_otp = pyotp.TOTP(secret_key).verify(otp)

        if (verify_otp):
            token = signJWT(user_id)
            return {"response": "Authenticated", "token": token['access_token']}
        else:
            return {"response": "Incorrect OTP"}
    except:
        return {"message": "key does not exist"}
