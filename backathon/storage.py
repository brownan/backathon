import base64
import json

from django.core.files.storage import Storage

import requests

extra_headers = {
    'User-Agent': 'Backathon+Python/3'
}

# Timeout used in HTTP calls
TIMEOUT = 5

class B2Storage(Storage):
    def __init__(self,
                 account_id,
                 application_key,
                 bucket_id,
                 ):
        self.account_id = account_id
        self.application_key = application_key
        self.bucket_id = bucket_id

        self._session = requests.Session()
        self._session.headers.update(extra_headers)

        # These parameters are acquired by calls to the API and cached
        self.authorization_token = None
        self.api_url = None
        self.download_url = None
        self.upload_url = None
        self.upload_token = None

    def _authorize_account(self):
        """Calls b2_authorize_account to get a session authorization token

        If successful, sets self.authorization_token and self.api_url

        If unsuccessful, raises an IOError with a description of the error
        """
        response = self._session.post(
            "https://api.backblazeb2.com/b2api/v1/b2_authorize_account",
            headers={
                'Authorization': 'Basic {}'.format(
                    base64.b64encode("{}:{}".format(
                        self.account_id,
                        self.application_key
                    ))
                ),
            },
            timeout=TIMEOUT,
        )

        try:
            data = response.json()
        except ValueError:
            # Invalid or no JSON returned from response
            response.raise_for_status()
            raise IOError("Invalid json response from B2")

        if response.status_code != 200:
            raise IOError(data['message'])

        self.authorization_token = data['authorizationToken']
        self.api_url = data['apiUrl']

    def _call_api(self, api_name, data):
        """Calls the given API with the given data

        If the account hasn't been authorized yet, calls b2_authorize_account
        first to obtain the authorization token

        If successful, returns the response json object

        If unsuccessful, raises an IOError with a description of the error
        """
        if self.api_url is None or self.authorization_token is None:
            self._authorize_account()

        response = self._session.post(
            "{}/b2api/v1/{}".format(self.api_url, api_name),
            headers = {
                'Authorization': self.authorization_token,
            },
            data=json.dumps(data),
            timeout=TIMEOUT,
        )

        try:
            data = response.json()
        except ValueError:
            response.raise_for_status()
            raise IOError("Invalid json response from B2 on call to {}".format(api_name))

        if response.status_code == 401 and data['code'] == "expired_auth_token":
            # Auth token has expired. Retry after getting a new one.
            self.api_url = None
            self.authorization_token = None
            return self._call_api(api_name, data)

        if response.status_code != 200:
            raise IOError(data['message'])

        return data
