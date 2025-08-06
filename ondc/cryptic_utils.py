import base64
import datetime
import os
import re
import json
import fire
import nacl.encoding
import nacl.hash
from nacl.bindings import crypto_sign_ed25519_sk_to_seed
from nacl.signing import SigningKey, VerifyKey
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives import serialization
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from dotenv import load_dotenv
load_dotenv()


def load_request_body():
    path = os.getenv("REQUEST_BODY_PATH", "request_body_raw_text.txt")
    if not os.path.exists(path):
        return None
    with open(path, "r") as f:
        request_body_raw_text = f.read()
    try:
        request_body_json = json.loads(request_body_raw_text)
        return json.dumps(request_body_json, separators=(',', ':'))
    except json.JSONDecodeError:
        return None


def hash_message(msg: str):
    HASHER = nacl.hash.blake2b
    digest = HASHER(bytes(msg, 'utf-8'), digest_size=64, encoder=nacl.encoding.Base64Encoder)
    return digest.decode("utf-8")


def create_signing_string(digest_base64, created=None, expires=None):
    created = created or int(datetime.datetime.now().timestamp())
    expires = expires or int((datetime.datetime.now() + datetime.timedelta(hours=1)).timestamp())
    signing_string = f"""(created): {created}
(expires): {expires}
digest: BLAKE-512={digest_base64}"""
    return signing_string


def sign_response(signing_key, private_key):
    private_key64 = base64.b64decode(private_key)
    seed = crypto_sign_ed25519_sk_to_seed(private_key64)
    signer = SigningKey(seed)
    signed = signer.sign(bytes(signing_key, encoding='utf8'))
    return base64.b64encode(signed.signature).decode()


def verify_response(signature, signing_key, public_key):
    try:
        public_key64 = base64.b64decode(public_key)
        VerifyKey(public_key64).verify(bytes(signing_key, 'utf8'), base64.b64decode(signature))
        return True
    except Exception:
        return False


def get_filter_dictionary_or_operation(filter_string):
    filter_string_list = [x.strip() for x in filter_string.split(',')]
    return {kv.split('=')[0].strip(): kv.split('=')[1].strip().replace("\"", "") for kv in filter_string_list}


def create_authorisation_header(request_body=None, created=None, expires=None):
    request_body = request_body 
    if request_body is None:
        raise ValueError("Request body not found or invalid.")
    
    if created is None:
        created = int(datetime.datetime.now().timestamp()) - 1000

    if expires is None:
        expires = int((datetime.datetime.now() + datetime.timedelta(minutes=10)).timestamp())

    
    

    signing_key = create_signing_string(hash_message(request_body), created, expires)
    print("Signing Key:", signing_key)  # For debugging
    signature = sign_response(signing_key, private_key=os.getenv("Signing_private_key"))

    subscriber_id = os.getenv("SUBSCRIBER_ID", "buyer-app.ondc.org")
    unique_key_id = os.getenv("UNIQUE_KEY_ID", "207")
    header = (
        f'Signature keyId="{subscriber_id}|{unique_key_id}|ed25519",'
        f'algorithm="ed25519",created="{created}",expires="{expires}",'
        f'headers="(created) (expires) digest",signature="{signature}"'
    )
    return header


def verify_authorisation_header(auth_header, request_body_str=None, public_key=None):
    request_body_str = request_body_str or load_request_body()
    if request_body_str is None:
        return False

    public_key = public_key or os.getenv("PUBLIC_KEY")

    header_parts = get_filter_dictionary_or_operation(auth_header.replace("Signature ", ""))
    created = int(header_parts['created'])
    expires = int(header_parts['expires'])
    current_timestamp = int(datetime.datetime.now().timestamp())

    if not (created <= current_timestamp <= expires):
        return False

    signing_key = create_signing_string(hash_message(request_body_str), created=created, expires=expires)
    return verify_response(header_parts['signature'], signing_key, public_key=public_key)


def generate_key_pairs():
    signing_key = SigningKey.generate()
    private_key = base64.b64encode(signing_key._signing_key).decode()
    public_key = base64.b64encode(bytes(signing_key.verify_key)).decode()

    inst_private_key = X25519PrivateKey.generate()
    inst_public_key = inst_private_key.public_key()

    encryption_private_key = base64.b64encode(inst_private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )).decode()

    encryption_public_key = base64.b64encode(inst_public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )).decode()

    return {
        "Signing_private_key": private_key,
        "Signing_public_key": public_key,
        "Encryption_Privatekey": encryption_private_key,
        "Encryption_Publickey": encryption_public_key,
    }


def encrypt(encryption_private_key, encryption_public_key, _=None):
    private_key = serialization.load_der_private_key(
        base64.b64decode(encryption_private_key),
        password=None
    )
    public_key = serialization.load_der_public_key(
        base64.b64decode(encryption_public_key)
    )
    shared_key = private_key.exchange(public_key)
    cipher = AES.new(shared_key, AES.MODE_ECB)
    text = b'ONDC is a Great Initiative!!'
    return base64.b64encode(cipher.encrypt(pad(text, AES.block_size))).decode('utf-8')


def decrypt(encryption_private_key, encryption_public_key, cipherstring):
    private_key = serialization.load_der_private_key(
        base64.b64decode(encryption_private_key),
        password=None
    )
    public_key = serialization.load_der_public_key(
        base64.b64decode(encryption_public_key)
    )
    shared_key = private_key.exchange(public_key)
    cipher = AES.new(shared_key, AES.MODE_ECB)
    ciphertxt = base64.b64decode(cipherstring)
    return unpad(cipher.decrypt(ciphertxt), AES.block_size).decode('utf-8')


if __name__ == '__main__':
    fire.Fire()
