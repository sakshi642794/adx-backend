from fastapi import APIRouter, Request, Query

router = APIRouter()

@router.post("/internal/pi")
async def pi_ingest(request: Request, room_id: str = Query(default="arena")):
    from app.utils.pi_bridge import handle_pi_event
    data = await request.json()
    await handle_pi_event(data, room_id)
    return {"status": "ok"}