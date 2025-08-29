from src.airtraffic.config.config import settings
import requests
from database.redis_db import r

def get_token_redis():

    token = r.get("opensky_token")

    if not token:
        data = {
            'grant_type': 'client_credentials',
            'client_id': settings.username,
            'client_secret': settings.password,
        }

        a = requests.post(
            'https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token',
            data=data,
        )

        token = a.json()['access_token']

        r.set("opensky_token", token)
        r.expire("opensky_token", 1500)
    return token
