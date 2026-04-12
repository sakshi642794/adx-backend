from fastapi import APIRouter

router = APIRouter()


@router.get("/status")
async def commanding_officer_status():
    return {
        "module": "commanding_officer",
        "status": "stub",
        "message": "Commanding Officer — Person 2 will implement",
    }
