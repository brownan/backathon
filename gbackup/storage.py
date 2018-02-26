import base64

from django.core.files.storage import Storage

import requests
from django.utils.functional import cached_property

extra_headers = {
    'User-Agent': 'Gbackup+Python/3'
}

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

    def _authorize_account(self):
        response = self._session.post(
            "https://api.backblazeb2.com/b2api/v1/b2_authorize_account",
            headers={
                'Authorization': 'Basic {}'.format(
                    base64.b64encode("{}:{}".format(
                        self.account_id,
                        self.application_key
                    ))
                ),
            }
        )
        try:
            data = response.json()
        except ValueError:
            response.raise_for_status()
            raise IOError("Invalid json response from B2")

        if response.status_code != 200:
            raise IOError(data['message'])

        self.__dict__['auth_token'] = data['authorizationToken']
        self.__dict__['api_url'] = data['apiUrl']

    @property
    def auth_token(self):
        self._authorize_account()
        return self.__dict__['auth_token']

    @property
    def api_url(self):
        return self.__dict__['api_url']

    @cached_property
    def upload_url(self):
        response = self._session.post(
            self.api_url,
            headers={
                'Authorization': self.auth_token,
            },
            json={
                'bucketId': self.bucket_id
            }
        )
        if response.status_code == 200:
            return response.json()['uploadUrl']
