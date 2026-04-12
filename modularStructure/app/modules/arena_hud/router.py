from fastapi import APIRouter

router = APIRouter()


@router.get("/status")
async def arena_hud_status():
    return {
        "module": "arena_hud",
        "status": "stub",
        "message": "Arena HUD — Person 2 will implement",
    }
