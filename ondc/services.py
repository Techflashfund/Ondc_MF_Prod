import base64
from nacl.signing import SigningKey
from nacl.bindings import crypto_sign_ed25519_sk_to_seed

# Base64 encoded full private key (private + public)
PRIVATE_KEY_BASE64 = "RlN6KBUkq0SWQiML4Y4jJ7y407eZsyFIGU1cZpAKiKfHTf0Ccb0CtKRbZhLw4Qv0iljcMUNNe5bVWOrdu+d9Ow=="

def sign_request_id(request_id: str, private_key_base64: str = PRIVATE_KEY_BASE64) -> str:
    """
    Signs a given request_id using an Ed25519 private key and returns a base64 encoded signature.
    
    Args:
        request_id (str): The request ID to sign.
        private_key_base64 (str): Base64-encoded Ed25519 full private key (default is from constant).

    Returns:
        str: Base64-encoded signature.
    """
    # Decode the base64 key and extract seed
    private_key_bytes = base64.b64decode(private_key_base64)
    seed = crypto_sign_ed25519_sk_to_seed(private_key_bytes)
    
    # Create signing key and sign the request_id
    signing_key = SigningKey(seed)
    signed = signing_key.sign(request_id.encode())
    
    # Return base64-encoded signature
    return base64.b64encode(signed.signature).decode()


