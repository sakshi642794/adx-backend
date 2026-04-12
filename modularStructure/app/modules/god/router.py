from fastapi import APIRouter

router = APIRouter()


@router.get("/status")
async def god_status():
    return {
        "module": "god",
        "status": "stub",
        "message": "GOD module — Person 2 will implement",
    }
