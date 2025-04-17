from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

class EchoRequest(BaseModel):
    text: str

app = FastAPI(title="Finance News Bot")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],            # or ["null"] to be more strict
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/echo")
async def echo(req: EchoRequest):
    return {"echo": req.text}
