import os
import requests
import secrets
from token_storage import TokenStorage


class KommoClient:
    def __init__(self, token_storage=None):
        self.storage = token_storage or TokenStorage()

    @property
    def client_id(self):
        return os.environ.get('KOMMO_CLIENT_ID', '')

    @property
    def client_secret(self):
        return os.environ.get('KOMMO_CLIENT_SECRET', '')

    @property
    def redirect_uri(self):
        return os.environ.get('KOMMO_REDIRECT_URI', '')

    @property
    def subdomain(self):
        return os.environ.get('KOMMO_SUBDOMAIN', '')

    def _base_url(self, subdomain=None):
        sub = subdomain or self.subdomain
        token = self.storage.get_token()
        if token and token['subdomain']:
            sub = token['subdomain']
        return f"https://{sub}.kommo.com"

    # --- OAuth2 ---

    def get_authorization_url(self, subdomain=None):
        sub = subdomain or self.subdomain
        state = secrets.token_urlsafe(16)
        url = (
            f"https://www.kommo.com/oauth"
            f"?client_id={self.client_id}"
            f"&state={state}"
            f"&mode=post_message"
        )
        return url, state, sub

    def exchange_code(self, code, subdomain=None):
        sub = subdomain or self.subdomain
        resp = requests.post(
            f"https://{sub}.kommo.com/oauth2/access_token",
            json={
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'grant_type': 'authorization_code',
                'code': code,
                'redirect_uri': self.redirect_uri,
            },
            headers={'Content-Type': 'application/json'},
        )
        resp.raise_for_status()
        data = resp.json()
        self.storage.save_token(
            access_token=data['access_token'],
            refresh_token=data['refresh_token'],
            expires_in=data['expires_in'],
            subdomain=sub,
        )
        return data

    def refresh_access_token(self):
        token = self.storage.get_token()
        if not token:
            raise Exception("Nenhum token salvo. Faça login novamente.")
        resp = requests.post(
            f"https://{token['subdomain']}.kommo.com/oauth2/access_token",
            json={
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'grant_type': 'refresh_token',
                'refresh_token': token['refresh_token'],
                'redirect_uri': self.redirect_uri,
            },
            headers={'Content-Type': 'application/json'},
        )
        resp.raise_for_status()
        data = resp.json()
        self.storage.save_token(
            access_token=data['access_token'],
            refresh_token=data['refresh_token'],
            expires_in=data['expires_in'],
            subdomain=token['subdomain'],
        )
        return data

    # --- API helpers ---

    def _get_headers(self):
        if self.storage.is_expired():
            self.refresh_access_token()
        token = self.storage.get_token()
        return {'Authorization': f"Bearer {token['access_token']}"}

    def _api_get(self, endpoint, params=None):
        url = f"{self._base_url()}/api/v4{endpoint}"
        resp = requests.get(url, headers=self._get_headers(), params=params)
        resp.raise_for_status()
        return resp.json()

    # --- API methods ---

    def get_account(self):
        return self._api_get('/account', params={'with': 'amojo_id'})

    def get_leads(self, limit=250, page=1):
        return self._api_get('/leads', params={'limit': limit, 'page': page, 'with': 'contacts'})

    def get_pipelines(self):
        return self._api_get('/leads/pipelines')

    def get_contacts(self, limit=250, page=1):
        return self._api_get('/contacts', params={'limit': limit, 'page': page})

    def get_users(self):
        return self._api_get('/users')

    def is_authenticated(self):
        token = self.storage.get_token()
        return token is not None
