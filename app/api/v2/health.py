from fastapi import APIRouter

router = APIRouter(tags=["health"])

@router.get("/health")
def health():
    return {"status": "Welcome to the Gas Data Pipeline API !! "}
