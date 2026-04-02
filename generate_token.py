import datetime
import os

import jwt
from dotenv import load_dotenv

load_dotenv()

secret = os.getenv("MCP_SERVER_JWT_SECRET")
issuer = os.getenv("MCP_SERVER_JWT_ISSUER", "")
audience = os.getenv("MCP_SERVER_JWT_AUDIENCE", "")

if not secret:
    print("Error: MCP_SERVER_JWT_SECRET not set in .env")
    print("Create a .env file with MCP_SERVER_JWT_SECRET first.")
    exit(1)

payload = {
    "sub": "test-user",
    "iat": datetime.datetime.now(datetime.timezone.utc),
    "exp": datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=365),
}

if issuer:
    payload["iss"] = issuer
if audience:
    payload["aud"] = audience

token = jwt.encode(payload, secret, algorithm="HS256")

print("Put this token in the Api Key field in Intric")
print("JWT Token (valid for 1 year):")
print(token)
print()
