from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from ..auth import create_token, decode_token, verify_password
from ..database import get_db
from ..models import LoginRequest, TokenResponse, UserResponse

router = APIRouter()
bearer = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer),
) -> str:
    username = decode_token(credentials.credentials)
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )
    return username


@router.post("/auth/login", response_model=TokenResponse)
async def login(body: LoginRequest, db=Depends(get_db)):
    async with db.cursor() as cur:
        await cur.execute(
            "SELECT password_hash FROM users WHERE username = %s", (body.username,)
        )
        row = await cur.fetchone()

    if not row or not verify_password(body.password, row[0]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials"
        )
    return TokenResponse(access_token=create_token(body.username))


@router.get("/auth/me", response_model=UserResponse)
async def me(username: str = Depends(get_current_user)):
    return UserResponse(username=username)
