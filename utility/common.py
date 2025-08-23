import base64
import re

from fastapi import HTTPException
from jose import ExpiredSignatureError, JWTError, jwt

from dependencies.configuration import Configuration
from dependencies.logger import logger
from Crypto.Cipher import AES, PKCS1_OAEP


class CommonUtils:

    @staticmethod
    def determine_environment(host: str) -> str:
        """Determine the environment based on the host."""
        env = "Dev"
        if "apidemo.aureolesofti.com" in host:
            env = "Demo"
        elif "api.aureolesofti.com" in host:
            env = "Prod"

        logger.info(f"Environment set to {env}")
        Configuration.init_config(env)
        return env

    @staticmethod
    def sanitize_and_validate_pan(pan_raw: str) -> str | None:

        if not pan_raw:
            return None

        # Remove any non-alphanumeric characters
        pan_clean = re.sub(r"[^A-Za-z0-9]", "", pan_raw).upper()

        if len(pan_clean) != 10:
            return None

        if not re.match(r"^[A-Z]{5}[0-9]{4}[A-Z]$", pan_clean):
            return None

        return pan_clean

    def decrypt_data(self, encrypted_data: str, secret_key: bytes, key_size: int = 128) -> str:
        """Decrypts the AES encrypted data.
        Allows customization for AES key size (128 or 256 bits).
        """
        encrypted_bytes = base64.b64decode(encrypted_data)
        cipher = AES.new(secret_key, AES.MODE_ECB)
        decrypted = cipher.decrypt(encrypted_bytes)
        return self.unpad_data(decrypted).decode('utf-8')

    def bearer_token_function(self, bearer_token: str):
        """
        Decode JWT, decrypt client_id and client_secret, and return Basic Auth string.
        """
        try:
            payload = jwt.decode(
                bearer_token,
                Configuration.JWT_SECRET,
                algorithms=["HS256"]
            )

            encrypted_client_id = payload.get("client_id")
            encrypted_client_secret = payload.get("client_secret")
            ent_id = payload.get("cid")

            if not encrypted_client_id or not encrypted_client_secret:
                raise HTTPException(status_code=401, detail="JWT missing required fields")

            AES_SECRET_KEY = b'dXNlcl9tYW5hZ2Vt'

            # Decrypt the values
            client_id = self.decrypt_data(encrypted_client_id, AES_SECRET_KEY)
            client_secret = self.decrypt_data(encrypted_client_secret, AES_SECRET_KEY)

            # Return as Base64 encoded Basic Auth
            basic_auth_bytes = f"{client_id}:{client_secret}".encode("utf-8")
            base_64_encode = base64.b64encode(basic_auth_bytes).decode("utf-8")
            return ent_id, client_id, client_secret, base_64_encode

        except ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="JWT token has expired")
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid JWT token")

    @staticmethod
    def unpad_data(data: bytes) -> bytes:
        """Removes PKCS#7 padding from the data."""
        padding_length = data[-1]
        return data[:-padding_length]