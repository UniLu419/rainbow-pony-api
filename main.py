from fastapi import FastAPI
import uvicorn
from app.routers import users
from app.config import Config

app = FastAPI()
app.include_router(users.router, prefix="/users", tags=["users"])


@app.get("/")
def root():
    return {"message": "Hello, FastAPI!"}


if __name__ == "__main__":
    uvicorn.run(app, host=Config.HOST, port=Config.PORT)
