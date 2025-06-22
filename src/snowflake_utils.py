import toml
from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def get_snowpark_session_from_config(connection_name="my_cortex_conn", config_path="config.toml") -> Session:
    # Load TOML config
    with open(config_path, "r") as f:
        config = toml.load(f)

    if connection_name not in config:
        raise ValueError(f"Connection '{connection_name}' not found in {config_path}")

    cfg = config[connection_name]

    # Load private key
    with open(cfg["private_key_path"], "rb") as key_file:
        private_key_bytes = key_file.read()

    p_key = serialization.load_pem_private_key(
        private_key_bytes,
        password=cfg["private_key_passphrase"].encode("utf-8"),
        backend=default_backend()
    )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    # Snowflake connection parameters
    connection_parameters = {
        "account": cfg["account"],
        "user": cfg["user"],
        "warehouse": cfg["warehouse"],
        "database": cfg["database"],
        "schema": cfg["schema"],
        "private_key": pkb,
        "role": cfg.get("role")
    }

    return Session.builder.configs(connection_parameters).create()
