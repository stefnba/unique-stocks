from prefect.blocks.system import Secret
from prefect_aws import AwsClientParameters, AwsCredentials
from pydantic import SecretStr

AwsCredentials(
    aws_access_key_id="test",
    aws_secret_access_key=SecretStr("test1234"),
    aws_client_parameters=AwsClientParameters(endpoint_url="http://localhost:9000"),
).save("aws-credentials", overwrite=True)


Secret(value=SecretStr("Secret")).save("eod-api-key", overwrite=True)
