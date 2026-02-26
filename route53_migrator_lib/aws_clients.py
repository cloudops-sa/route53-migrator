from typing import Any, Dict

import boto3
from botocore.exceptions import BotoCoreError, ClientError, ProfileNotFound


def get_session(profile_name: str) -> boto3.Session:
    try:
        return boto3.Session(profile_name=profile_name)
    except ProfileNotFound as e:
        raise RuntimeError(
            f"AWS profile '{profile_name}' not found. Configure it in ~/.aws/config or set AWS_PROFILE."
        ) from e


def assert_profile_usable(profile_name: str) -> Dict[str, Any]:
    session = get_session(profile_name)
    try:
        sts = session.client("sts")
        return sts.get_caller_identity()
    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(
            f"Unable to use AWS profile '{profile_name}'. Verify credentials and permissions."
        ) from e


def route53_client(profile_name: str):
    session = get_session(profile_name)
    return session.client("route53")


def s3_client(profile_name: str):
    session = get_session(profile_name)
    return session.client("s3")
