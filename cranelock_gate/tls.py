"""Generate DEVELOPMENT mutual-TLS material for the gate, its clients and health probes.

    python -m cranelock_gate.tls --directory demo-data/tls

Writes ca.pem, server.pem/server-key.pem, client.pem/client-key.pem and
probe.pem/probe-key.pem. The CA private key is never written, so issuing more
certificates means regenerating the set. Use a real PKI outside simulations.
"""
import argparse
import datetime
import ipaddress
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

# Names a client may dial: loopback, the Compose service and the Kubernetes Service.
DEFAULT_NAMES = ["localhost", "127.0.0.1", "gate", "cranelock-gate",
                 "cranelock-gate.default.svc", "cranelock-gate.default.svc.cluster.local"]
FILES = ["ca.pem", "server.pem", "server-key.pem", "client.pem", "client-key.pem",
         "probe.pem", "probe-key.pem"]


def _name(common_name):
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])


def _key_pem(key):
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                             serialization.NoEncryption())


def generate(names=DEFAULT_NAMES, days=30):
    """Return {filename: PEM bytes} for a fresh CA, server, client and probe identity."""
    now = datetime.datetime.now(datetime.timezone.utc)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = _name("CraneLock development CA")

    def issue(subject, key, usage=None, sans=()):
        builder = (x509.CertificateBuilder().subject_name(subject).issuer_name(ca_name)
                   .public_key(key.public_key()).serial_number(x509.random_serial_number())
                   .not_valid_before(now - datetime.timedelta(minutes=5))
                   .not_valid_after(now + datetime.timedelta(days=days))
                   .add_extension(x509.BasicConstraints(ca=usage is None, path_length=None),
                                  critical=True))
        if usage is not None:
            builder = builder.add_extension(x509.ExtendedKeyUsage([usage]), critical=False)
        if sans:
            builder = builder.add_extension(x509.SubjectAlternativeName(list(sans)), critical=False)
        return builder.sign(ca_key, hashes.SHA256()).public_bytes(serialization.Encoding.PEM)

    sans = []
    for name in names:
        try:
            sans.append(x509.IPAddress(ipaddress.ip_address(name)))
        except ValueError:
            sans.append(x509.DNSName(name))
    files = {"ca.pem": issue(ca_name, ca_key)}
    for role, usage in (("server", ExtendedKeyUsageOID.SERVER_AUTH),
                        ("client", ExtendedKeyUsageOID.CLIENT_AUTH),
                        ("probe", ExtendedKeyUsageOID.CLIENT_AUTH)):
        key = ec.generate_private_key(ec.SECP256R1())
        files[f"{role}.pem"] = issue(_name(f"cranelock-{role}"), key, usage,
                                     sans if role == "server" else ())
        files[f"{role}-key.pem"] = _key_pem(key)
    return files


def write(directory, names=DEFAULT_NAMES, days=30):
    directory = Path(directory)
    if any((directory / name).exists() for name in FILES):
        raise FileExistsError(f"TLS files already exist in {directory}; refusing to overwrite")
    directory.mkdir(parents=True, exist_ok=True)
    for name, content in generate(names, days).items():
        (directory / name).write_bytes(content)
    return directory


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--directory", default="demo-data/tls")
    parser.add_argument("--name", action="append", dest="names",
                        help="Server DNS name or IP (repeatable); replaces the defaults")
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args()
    directory = write(args.directory, args.names or DEFAULT_NAMES, args.days)
    print(f"Development TLS files written to {directory}. Keep the *-key.pem files private.")


if __name__ == "__main__":
    main()
