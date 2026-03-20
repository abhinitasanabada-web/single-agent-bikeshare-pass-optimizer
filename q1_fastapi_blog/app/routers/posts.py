from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.deps import get_current_user, get_db, require_roles
from app.models import Post, User
from app.schemas import PostCreate, PostRead

router = APIRouter(prefix="/posts", tags=["posts"])


@router.get("", response_model=list[PostRead])
def list_posts(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[PostRead]:
    del current_user
    return db.query(Post).order_by(Post.id.asc()).all()


@router.post(
    "",
    response_model=PostRead,
    status_code=status.HTTP_201_CREATED,
)
def create_post(
    payload: PostCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("writer", "moderator")),
) -> PostRead:
    post = Post(
        title=payload.title,
        content=payload.content,
        author_id=current_user.id,
    )
    db.add(post)
    db.commit()
    db.refresh(post)
    return post


@router.delete("/{post_id}")
def delete_post(
    post_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("moderator")),
) -> dict[str, str]:
    del current_user
    post = db.query(Post).filter(Post.id == post_id).first()
    if post is None:
        raise HTTPException(status_code=404, detail="Post not found")

    db.delete(post)
    db.commit()
    return {"message": f"Post {post_id} deleted successfully"}
