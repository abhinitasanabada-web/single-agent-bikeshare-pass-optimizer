from __future__ import annotations

from app.core.database import Base, SessionLocal, engine
from app.core.security import get_password_hash
from app.models import Post, User

SEED_USERS = [
    {"username": "reader1", "password": "reader123", "role": "reader"},
    {"username": "writer1", "password": "writer123", "role": "writer"},
    {"username": "mod1", "password": "mod123", "role": "moderator"},
]


def main() -> None:
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        for item in SEED_USERS:
            existing = db.query(User).filter(User.username == item["username"]).first()
            if existing:
                continue
            db.add(
                User(
                    username=item["username"],
                    password=get_password_hash(item["password"]),
                    role=item["role"],
                )
            )

        db.commit()

        if db.query(Post).count() == 0:
            writer = db.query(User).filter(User.username == "writer1").first()
            if writer:
                db.add(
                    Post(
                        title="Welcome post",
                        content="This is the first seeded post.",
                        author_id=writer.id,
                    )
                )
                db.commit()
    finally:
        db.close()


if __name__ == "__main__":
    main()
