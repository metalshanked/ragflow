"""
MinIO Client Test App — RAGFlow Style

This script demonstrates how RAGFlow connects to MinIO and extends it with
SSL/TLS options that RAGFlow does NOT currently expose:

  1. No SSL          — exactly how RAGFlow works today (secure=False).
  2. SSL + skip verify — useful when MinIO uses a self-signed cert and you
                         just want to get going quickly.
  3. SSL + custom CA  — the proper way to trust a self-signed / internal CA
                         certificate.

Prerequisites:
    pip install minio==7.2.4   (same version RAGFlow pins in pyproject.toml)

Usage:
    python minio_client_test.py
"""

import io
import ssl
import logging
import urllib3
from minio import Minio
from minio.error import S3Error

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

# ==========================================
# CONFIGURATION  — edit these to match your environment
# ==========================================
MINIO_HOST     = "localhost:9000"   # host:port (no scheme)
MINIO_USER     = "rag_flow"
MINIO_PASSWORD = "infini_rag_flow"
TEST_BUCKET    = "ragflow-test-bucket"
TEST_OBJECT    = "hello.txt"
TEST_DATA      = b"Hello from RAGFlow MinIO test!"

# Path to your self-signed CA bundle (PEM).
# Only needed for Option 3 below.
CUSTOM_CA_CERT = "/path/to/ca-bundle.crt"

# ==========================================
# OPTION 1 — No SSL  (how RAGFlow does it today)
# ==========================================
# Source: rag/utils/minio_conn.py  lines 86-89
#
#   self.conn = Minio(settings.MINIO["host"],
#                     access_key=settings.MINIO["user"],
#                     secret_key=settings.MINIO["password"],
#                     secure=False)
#
# RAGFlow hardcodes `secure=False`, so all traffic is plain HTTP.
# This is fine when MinIO runs inside the same Docker network.

def create_client_no_ssl() -> Minio:
    """Exactly how RAGFlow creates its MinIO client."""
    return Minio(
        MINIO_HOST,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=False,          # <-- plain HTTP, no TLS
    )


# ==========================================
# OPTION 2 — SSL but SKIP certificate verification
# ==========================================
# Useful for quick testing with self-signed certs.
# NOT recommended for production.
#
# The `minio` library accepts an `http_client` parameter.
# We build a urllib3.PoolManager that disables cert verification.

def create_client_ssl_no_verify() -> Minio:
    """Connect over HTTPS but do NOT verify the server certificate."""
    http_client = urllib3.PoolManager(
        cert_reqs="CERT_NONE",          # skip verification
        assert_hostname=False,          # don't check hostname either
    )
    # Suppress the InsecureRequestWarning that urllib3 emits
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    return Minio(
        MINIO_HOST,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=True,                    # use HTTPS
        http_client=http_client,        # ...but don't verify certs
    )


# ==========================================
# OPTION 3 — SSL with a custom / self-signed CA certificate
# ==========================================
# The correct approach for production when MinIO uses a cert
# signed by an internal CA or a self-signed cert.
#
# You can also pass `cert_file` and `key_file` for mutual TLS (mTLS).

def create_client_ssl_custom_ca(ca_cert_path: str = CUSTOM_CA_CERT) -> Minio:
    """Connect over HTTPS and verify using a custom CA bundle."""
    http_client = urllib3.PoolManager(
        cert_reqs="CERT_REQUIRED",      # enforce verification
        ca_certs=ca_cert_path,          # path to PEM CA bundle
    )
    return Minio(
        MINIO_HOST,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=True,                    # use HTTPS
        http_client=http_client,        # verify with custom CA
    )


# ==========================================
# BONUS — SSL with mutual TLS (client certificate)
# ==========================================
def create_client_mtls(
    ca_cert_path: str = CUSTOM_CA_CERT,
    client_cert_path: str = "/path/to/client.crt",
    client_key_path: str = "/path/to/client.key",
) -> Minio:
    """Connect over HTTPS with mutual TLS (both sides present certs)."""
    http_client = urllib3.PoolManager(
        cert_reqs="CERT_REQUIRED",
        ca_certs=ca_cert_path,
        cert_file=client_cert_path,
        key_file=client_key_path,
    )
    return Minio(
        MINIO_HOST,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=True,
        http_client=http_client,
    )


# ==========================================
# RAGFlow-style helper operations (mirrors rag/utils/minio_conn.py)
# ==========================================

def ensure_bucket(client: Minio, bucket: str):
    """Create bucket if it does not exist (RAGFlowMinio.put does this)."""
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        logging.info("Created bucket: %s", bucket)
    else:
        logging.info("Bucket already exists: %s", bucket)


def put_object(client: Minio, bucket: str, name: str, data: bytes):
    """Upload bytes — mirrors RAGFlowMinio.put (retries omitted for clarity)."""
    result = client.put_object(bucket, name, io.BytesIO(data), len(data))
    logging.info("PUT  %s/%s  etag=%s", bucket, name, result.etag)
    return result


def get_object(client: Minio, bucket: str, name: str) -> bytes:
    """Download bytes — mirrors RAGFlowMinio.get."""
    response = client.get_object(bucket, name)
    data = response.read()
    response.close()
    response.release_conn()
    logging.info("GET  %s/%s  size=%d bytes", bucket, name, len(data))
    return data


def object_exists(client: Minio, bucket: str, name: str) -> bool:
    """Check existence — mirrors RAGFlowMinio.obj_exist."""
    try:
        client.stat_object(bucket, name)
        return True
    except S3Error as e:
        if e.code in ("NoSuchKey", "NoSuchBucket", "ResourceNotFound"):
            return False
        raise


def remove_object(client: Minio, bucket: str, name: str):
    """Delete an object — mirrors RAGFlowMinio.rm."""
    client.remove_object(bucket, name)
    logging.info("RM   %s/%s", bucket, name)


def health_check(client: Minio) -> bool:
    """Quick connectivity test — mirrors RAGFlowMinio.health."""
    try:
        client.list_buckets()
        return True
    except Exception as exc:
        logging.warning("Health check failed: %s", exc)
        return False


# ==========================================
# MAIN — run the demo
# ==========================================

def run_demo(client: Minio, label: str):
    print(f"\n{'=' * 50}")
    print(f"  {label}")
    print(f"{'=' * 50}")

    # Health
    ok = health_check(client)
    print(f"  Health check : {'OK' if ok else 'FAIL'}")
    if not ok:
        print("  Skipping remaining operations (MinIO unreachable).")
        return

    # Bucket
    ensure_bucket(client, TEST_BUCKET)

    # Put
    put_object(client, TEST_BUCKET, TEST_OBJECT, TEST_DATA)

    # Exists?
    exists = object_exists(client, TEST_BUCKET, TEST_OBJECT)
    print(f"  Object exists: {exists}")

    # Get
    fetched = get_object(client, TEST_BUCKET, TEST_OBJECT)
    assert fetched == TEST_DATA, "Data mismatch!"
    print(f"  Data matches : True")

    # Remove
    remove_object(client, TEST_BUCKET, TEST_OBJECT)
    print(f"  Cleaned up   : True")


if __name__ == "__main__":
    # ------------------------------------------------------------------
    # Choose which client to test.  Uncomment ONE of the lines below.
    # ------------------------------------------------------------------

    # Option 1 — No SSL (default RAGFlow behavior)
    run_demo(create_client_no_ssl(), "Option 1: No SSL (RAGFlow default)")

    # Option 2 — SSL, skip certificate verification
    # run_demo(create_client_ssl_no_verify(), "Option 2: SSL — skip verify")

    # Option 3 — SSL with custom CA cert
    # run_demo(create_client_ssl_custom_ca("/path/to/ca-bundle.crt"),
    #          "Option 3: SSL — custom CA cert")

    # Bonus — Mutual TLS
    # run_demo(create_client_mtls("/path/to/ca.crt",
    #                             "/path/to/client.crt",
    #                             "/path/to/client.key"),
    #          "Bonus: Mutual TLS")
