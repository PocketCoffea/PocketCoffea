import socket
import os
import subprocess


def check_port(port):
    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("0.0.0.0", port))
        available = True
    except Exception:
        available = False
    sock.close()
    return available


def get_proxy_path() -> str:
    '''
    Checks if the VOMS proxy exists and if it is valid
    for at least 1 hour.
    If it exists, returns the path of it'''
    try:
        subprocess.run("voms-proxy-info -exists -hours 1", shell=True, check=True)
    except subprocess.CalledProcessError:
        raise Exception(
            "VOMS proxy expirend or non-existing: please run `voms-proxy-init -voms cms -rfc --valid 168:0`"
        )

    # Now get the path of the certificate
    proxy = subprocess.check_output(
        "voms-proxy-info -path", shell=True, text=True
    ).strip()
    return proxy
