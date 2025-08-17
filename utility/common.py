import re

from dependencies.configuration import Configuration
from dependencies.logger import logger


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
