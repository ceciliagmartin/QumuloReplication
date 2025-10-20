#!/usr/bin/python3
################################################################################

from dataclasses import dataclass, field
from typing import TypedDict, List, Dict, Set, Optional
from qumulo.rest_client import RestClient
from qumulo.lib.auth import Credentials
from qumulo.lib.request import RequestError

import argparse
import csv
import getpass
import sys
import logging


class Creds(TypedDict):
    QHOST: str
    QUSER: str
    QPASS: str
    QPORT: int
    QTOKEN: str


# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def create_credentials(host: str, user: str, password: Optional[str] = None, port: int = 8000) -> Creds:
    """
    Create credentials dictionary with password prompt if not provided

    Args:
        host: Cluster hostname or IP
        user: Username
        password: Password (will prompt if None)
        port: API port (default: 8000)

    Returns:
        Creds dictionary ready for Client initialization
    """
    if not password:
        password = getpass.getpass(f"Enter password for {user}@{host}: ")

    return {
        "QHOST": host,
        "QUSER": user,
        "QPASS": password,
        "QPORT": port,
    }


class Client:
    def __init__(self, creds: Creds):
        self.creds = creds
        self.rc = None
        self.login()

    def login(self):
        if "QTOKEN" in self.creds:
            self.token_login()
        else:
            self.user_login()

    def token_login(self) -> None:
        try:
            self.rc = RestClient(
                address=self.creds["QHOST"],
                port=self.creds["QPORT"],
                credentials=Credentials(self.creds["QTOKEN"]),
            )
            logger.info("Successfully logged in with authentication token.")
        except Exception as e:
            logger.error(f"Token login failed: {e}")
            sys.exit(1)

    def user_login(self) -> None:
        try:
            self.rc = RestClient(address=self.creds["QHOST"], port=self.creds["QPORT"])
            self.rc.login(self.creds["QUSER"], self.creds["QPASS"])
            logger.info("Successfully logged in with username and password.")
        except Exception as e:
            logger.error(f"User login failed: {e}")
            sys.exit(1)


def main() -> None:
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Qumulo cluster credentials")
    parser.add_argument(
        "--host", type=str, required=True, help="Qumulo node IP address or FQDN"
    )
    parser.add_argument("--username", type=str, help="Username for the Qumulo cluster")
    parser.add_argument("--password", type=str, help="Password for the Qumulo cluster")
    parser.add_argument(
        "--token", type=str, help="Authentication token for the Qumulo cluster"
    )
    parser.add_argument(
        "--basepath", type=str, help="Base file system path to create subsnapshots"
    )
    args = parser.parse_args()

    if not args.token and not args.username:
        parser.error("Either --username or --token must be provided.")

    creds: Creds = {
        "QHOST": args.host,
        "QPORT": 8000,  # Default Qumulo REST API port
    }

    if args.token:
        creds["QTOKEN"] = args.token
    else:
        password = (
            args.password if args.password else getpass.getpass("Enter your password: ")
        )
        creds["QUSER"] = args.username
        creds["QPASS"] = password

    logger.info(f"Querying base path {args.basepath}")
    client = Client(creds)
    results = client.rc.fs.enumerate_entire_directory(path=args.basepath)
   
   # import pdb; pdb.set_trace()
    

#        import pdb; pdb.set_trace()
# logger.info(
#    f"Total capacity reported {snapshot.format_bytes(int(snapshot.rc.snapshot.get_total_used_capacity()['bytes']))}"
# )


if __name__ == "__main__":
    main()
