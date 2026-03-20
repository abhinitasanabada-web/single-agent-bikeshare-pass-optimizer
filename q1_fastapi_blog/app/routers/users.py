from __future__ import annotations

from fastapi import APIRouter, Depends

from app.deps import get_current_user
from app.models import User
from app.schemas import UserRead

router = APIRouter(prefix="/users", tags=["users"])


@router.get("/me", response_model=UserRead)
def read_me(current_user: User = Depends(get_current_user)) -> UserRead:
    return current_user
