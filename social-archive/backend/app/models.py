from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserResponse(BaseModel):
    username: str


class ItemResponse(BaseModel):
    id: int
    title: Optional[str] = None
    uri: Optional[str] = None
    body: Optional[str] = None
    source_context: Optional[str] = None
    type: Optional[str] = None
    summary: Optional[str] = None
    tags: list[str] = []
    flagged_for_deletion: bool = False
    saved_at: Optional[datetime] = None
    time_estimate: Optional[str] = None
    estimate_reasoning: Optional[str] = None


class ItemsResponse(BaseModel):
    items: list[ItemResponse]
    total: int
    limit: int
    offset: int


class FlagRequest(BaseModel):
    flagged_for_deletion: bool


class EditRequest(BaseModel):
    title: Optional[str] = None
    tags: Optional[list[str]] = None


class BookmarkItem(BaseModel):
    title: Optional[str] = None
    uri: str
    source: str
    source_context: Optional[str] = None
    tags: list[str] = []


class BookmarkSyncRequest(BaseModel):
    bookmarks: list[BookmarkItem]


class BookmarkSyncResponse(BaseModel):
    inserted: int
    skipped: int
