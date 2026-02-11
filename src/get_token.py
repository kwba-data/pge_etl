import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from urllib3.util import Retry
from typing import Dict


def create_session() -> requests.Session:
    """Creates a request session with predetermined retry logic"""
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    return session


def get_access_token(client_id, client_secret) -> Dict:
    url = (
        "https://api.pge.com/datacustodian/oauth/v2/token?grant_type=client_credentials"
    )

    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        response = requests.post(
            url=url,
            headers=headers,
            auth=HTTPBasicAuth(client_id, client_secret),
            cert=("cert/certificate.txt", "cert/private_key_1.txt"),
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            raise Exception(f"Error returned: {data['error']}")
        return data
    except requests.Timeout as e:
        raise Exception(f"Request timed out: {e}")
    except requests.RequestException as e:
        raise Exception(f"Request failed: {e}")
