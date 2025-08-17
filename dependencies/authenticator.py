import base64

from dependencies.logger import logger
from models.ent_client import ClientService


class Authenticator:

    @staticmethod
    def validate(headers, db_session,  service_id=None):

        auth_token = headers.get('Authorization') or headers.get('authorization')
        if not auth_token:
            logger.error('Authentication Token not found.')
            raise InterruptedError("401|AUTHENTICATION_FAILED")

        try:
            auth_token = auth_token.replace("Basic ", "")
            client_id, client_secret = base64.b64decode(auth_token).decode('ascii').split(":")
        except Exception as err:
            logger.exception(f'Base64 decoding failed. Authentication failed: {err}')
            raise InterruptedError("401|AUTHENTICATION_FAILED")

        try:
            query_param = {
                "client_id" : client_id.strip(),
                "client_secret" : client_secret.strip(),
                "status" :'enabled'
            }

            if service_id:
                query_param.update({"service_id": service_id})

            client = db_session.query(ClientService).filter_by(**query_param).first()

            if not client:
                logger.error('No client found for the supplied Authentication Token.')
                raise InterruptedError("401|AUTHENTICATION_FAILED")

            logger.info(f'Authenticated client with eID: [{client.cid}]')

            return client.cid, auth_token
        except Exception as err:
            logger.exception('Exception occurred during authentication: %s', err)
            raise InterruptedError("401|AUTHENTICATION_FAILED")

        finally:
            db_session.close()
