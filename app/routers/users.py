from fastapi import APIRouter
from uuid import UUID
router = APIRouter()

@router.get("/{id}")
def get_user(id:UUID):
    return {"id":id}