from fastapi import FastAPI, Request
import uvicorn
from supabase import create_client
from contextlib import asynccontextmanager
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from pydantic import BaseModel
from utils.db import resolve_ward_from_address
load_dotenv('.env.local')
import os
import logging

logging.basicConfig(level=logging.ERROR)

class AddressRequest(BaseModel):
    address: str
    city: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        supabase = create_client(
            os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        )
        app.state.supabase = supabase
        yield
    except Exception as e:
        logging.error("Error during startup:", e)
    


app = FastAPI(lifespan=lifespan)

@app.get("/", status_code=200)
async def check_status():
    return JSONResponse(content={"status": "ok"})


@app.post("/resolve-ward")
async def resolve_ward(request: Request, addr_req: AddressRequest):
    supabase = request.app.state.supabase

    ward_info = resolve_ward_from_address(
        supabase,
        addr_req.address,
        addr_req.city
    )

    if ward_info:
        return JSONResponse(content={
            "ward_number": ward_info["ward_number"],
            "ward_name": ward_info["ward_name"],
            "mohalla": ward_info["matched_mohalla"],
            "score": ward_info["confidence"]
        })
    else:
        return JSONResponse(content={"error": "Ward not found"}, status_code=404)




if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9453)