# ruff: noqa: F403, F405, E402, E501, F401

from __future__ import annotations
from rich import print
from git import Repo
import pandas as pd
from env import *
import pickle
import subprocess
from ccdexplorer_fundamentals.GRPCClient.CCD_Types import CCD_AccountInfo
from ccdexplorer_fundamentals.GRPCClient import GRPCClient
from rich.console import Console
import time
import datetime as dt
from ccdexplorer_fundamentals.tooter import Tooter, TooterChannel, TooterType
from ccdexplorer_fundamentals.mongodb import (
    MongoDB,
    Collections,
    MongoMotor,
)

console = Console()


class Account:
    def __init__(self, account, block_hash, grpcclient: GRPCClient):
        """
        The Account class holds all relevant information for an account on chain.
        """
        self.account = account
        self.block_hash = block_hash
        self.grpcclient: GRPCClient = grpcclient
        self.process_grpc_account_info()

    def process_grpc_account_info(self):
        ai: CCD_AccountInfo = self.grpcclient.get_account_info(
            self.block_hash, self.account
        )

        self.index = ai.index
        self.total_balance = int(ai.amount)
        self.locked_balance = int(ai.schedule.total)
        self.unlocked_balance = self.total_balance - self.locked_balance
        self.nonce = ai.sequence_number
        if ai.credentials["0"].initial:
            self.credential_creation_date = f"{ai.credentials['0'].initial.policy.created_at.year}{ai.credentials['0'].initial.policy.created_at.month}"
            self.credential_valid_to_date = f"{ai.credentials['0'].initial.policy.valid_to.year}{ai.credentials['0'].initial.policy.valid_to.month}"
        else:
            self.credential_creation_date = f"{ai.credentials['0'].normal.policy.created_at.year}{ai.credentials['0'].normal.policy.created_at.month}"
            self.credential_valid_to_date = f"{ai.credentials['0'].normal.policy.valid_to.year}{ai.credentials['0'].normal.policy.valid_to.month}"

        self.credential_count = len(ai.credentials.keys())

        if ai.stake:
            if ai.stake.baker:
                self.baker = True
                self.baker_id = str(ai.stake.baker.baker_info.baker_id)
                self.staked_amount = int(ai.stake.baker.staked_amount)
                self.restake_earnings = ai.stake.baker.restake_earnings
                self.pool_status = ai.stake.baker.pool_info.open_status
                self.pool_metadata_url = ai.stake.baker.pool_info.url
                self.pool_transaction_commission = (
                    ai.stake.baker.pool_info.commission_rates.transaction
                )
                self.pool_finalization_commission = (
                    ai.stake.baker.pool_info.commission_rates.finalization
                )
                self.pool_baking_commission = (
                    ai.stake.baker.pool_info.commission_rates.baking
                )
            else:
                self.baker = False

            if ai.stake.delegator:
                self.delegator = True
                self.staked_amount = int(ai.stake.delegator.staked_amount)
                self.restake_earnings = ai.stake.delegator.restake_earnings
                if ai.stake.delegator.target.passive_delegation:
                    self.delegation_target = "passiveDelegation"
                else:
                    self.delegation_target = ai.stake.delegator.target.baker

            else:
                self.delegator = False
        else:
            self.baker = None
            self.delegator = None


class Daily:
    def __init__(
        self,
        date: str,
        block_hash: str,
        grpcclient: GRPCClient,
        repo: Repo,
        new_dir: str,
        mongodb: MongoDB,
        tooter: Tooter,
    ):
        self.date = date
        self.new_dir = new_dir
        self.block_hash = block_hash
        self.grpcclient: GRPCClient = grpcclient
        self.repo = repo
        self.mongodb = mongodb
        self.tooter = tooter
        self.get_accounts_for_day()
        self.retrieve_account_info_for_day_on_server()
        self.save_accounts_for_day()
        self.git_push()

    def get_accounts_for_day(self):
        console.log(self.date, "Get accounts...")
        self.accounts = self.grpcclient.get_account_list(self.block_hash)

    def read_downloaded_accounts_from_disk(self):
        try:
            f = open(f"{self.block_hash}.pickle", "rb")
            pickle_contents = pickle.load(f)
            self.processed_accounts = pickle_contents
            f.close()
        except Exception as _:
            print("No pickle found for this day.")
            self.processed_accounts = {}

    def retrieve_account_info_for_day_on_server(self):
        console.log(self.date, f"Retrieve account info at {self.block_hash}...")
        console.log(
            self.date,
            f"{dt.datetime.now()}: Start of loop through all {len(self.accounts)} accounts...",
        )
        self.read_downloaded_accounts_from_disk()
        if self.processed_accounts == {}:
            results = []
            for index, account in enumerate(self.accounts):
                if (index % 10_000) == 0:
                    console.log(f"Working on account_index {index:,.0f}...")
                results.append(self._perform_account_action(account))
            self.processed_accounts = results

    def _perform_account_action(self, account):
        # after this step, acc holds the accountInfo for this account at the relevant block.
        acc = Account(account, self.block_hash, grpcclient)

        dd = {}
        dd["account"] = acc.account
        dd["nonce"] = acc.nonce
        dd["index"] = acc.index

        dd["credential_creation_date"] = dt.date(
            int(acc.credential_creation_date[:4]),
            int(acc.credential_creation_date[4:]),
            1,
        )
        dd["credential_valid_to_date"] = dt.date(
            int(acc.credential_valid_to_date[:4]),
            int(acc.credential_valid_to_date[4:]),
            1,
        )
        dd["credential_count"] = acc.credential_count
        dd["total_balance"] = acc.total_balance / 1_000_000
        dd["unlocked_balance"] = acc.unlocked_balance / 1_000_000
        dd["locked_balance"] = acc.locked_balance / 1_000_000

        dd["baker_id"] = acc.baker_id if acc.baker else None
        dd["staked_amount"] = acc.staked_amount / 1_000_000 if acc.baker else None
        dd["restake_earnings"] = acc.restake_earnings if acc.baker else None

        dd["pool_status"] = acc.pool_status if acc.baker else None
        dd["pool_metadata_url"] = acc.pool_metadata_url if acc.baker else None
        dd["pool_transaction_commission"] = (
            acc.pool_transaction_commission if acc.baker else None
        )
        dd["pool_finalization_commission"] = (
            acc.pool_finalization_commission if acc.baker else None
        )
        dd["pool_baking_commission"] = acc.pool_baking_commission if acc.baker else None

        if not (acc.baker):
            dd["staked_amount"] = (
                acc.staked_amount / 1_000_000 if acc.delegator else None
            )
            dd["restake_earnings"] = acc.restake_earnings if acc.delegator else None
            dd["delegation_target"] = acc.delegation_target if acc.delegator else None

        return dd

    def save_downloaded_accounts_to_disk(self):
        try:
            f = open(f"{self.block_hash}.pickle", "wb")
            save_object = self.processed_accounts
            pickle.dump(save_object, f)
            f.close()
        except Exception as e:
            print(e)

    def save_accounts_for_day(self):
        """
        This is the final step from the account retrieval and enrichment process,
        storing the results on disk in 'accounts.csv'.
        """
        console.log(self.date, "Save accounts...")
        df = pd.DataFrame(self.processed_accounts)
        self.df_accounts = df

        df.to_csv(f"{self.new_dir}/accounts.csv", index=False)

        # now also save to Mongodb...
        df["_id"] = df["account"]
        df["credential_creation_date"] = pd.to_datetime(df["credential_creation_date"])
        df["credential_valid_to_date"] = pd.to_datetime(df["credential_valid_to_date"])
        # all_records = df.to_dict('records')
        all_records = [
            {k: v for k, v in m.items() if pd.notnull(v)}
            for m in df.to_dict(orient="records")
        ]

        try:
            self.mongodb.mainnet[Collections.nightly_accounts].delete_many(
                {"account": {"$exists": True}}
            )
            self.mongodb.mainnet[Collections.nightly_accounts].insert_many(all_records)

            self.tooter.send(
                channel=TooterChannel.NOTIFIER,
                message=f"Accounts Retrieval: Nightly accounts saved to MongoDB for {self.date}.",
                notifier_type=TooterType.INFO,
            )
        except Exception as e:
            print(e)

    def git_push(self):
        console.log(self.date, "Git add, commit, push...")
        remote = "origin"
        try:
            console.log(self.date, f"{self.repo.remote(name=remote).exists()=}")
        except Exception as e:
            console.log(
                self.date,
                f"{e}: Remote bot_remote doesn't exist, trying to create and push...",
            )
            remote = self.repo.create_remote(
                remote,
                url=f"https://ceupdaterbot:{CE_BOT_TOKEN}@github.com/ccdexplorer/ccdexplorer-accounts",
            )
            console.log(self.date, "Remote bot_remote created...")
        try:
            self.repo.git.add("-A")
            self.repo.index.commit(self.date)
            origin = self.repo.remote(name=remote)

            origin.push(force_with_lease=True)
            query = {"_id": "last_known_nightly_accounts"}
            self.mongodb.mainnet[Collections.helpers].replace_one(
                query, {"date": self.date}, upsert=True
            )
        except Exception as e:
            print(f"Some error occured while pushing the code: {e}")


grpcclient = GRPCClient()
tooter = Tooter()
mongodb = MongoDB(tooter)
motormongo = MongoMotor(tooter)


if __name__ == "__main__":
    console.log(f"{ON_SERVER=}.")
    new_dir = "/Users/sander/Developer/open_source/ccdexplorer-accounts"
    if ON_SERVER:
        new_dir = "/home/git_dir"
    repo_new = Repo(new_dir)

    if ON_SERVER:
        new_dir = "/home/git_dir"
        result = subprocess.run(
            [
                "git",
                "-C",
                "/home/git_dir",
                "remote",
                "add",
                "bot_remote",
                f"https://ceupdaterbot:{CE_BOT_TOKEN}@github.com/ccdexplorer/ccdexplorer-accounts",
            ],
            stdout=subprocess.PIPE,
        )
        result = subprocess.run(
            [
                "git",
                "-C",
                "/home/git_dir",
                "remote",
                "-v",
            ],
            stdout=subprocess.PIPE,
        )
        console.log(
            "xxxx-xx-xx",
            f"Result of adding repo (statistics) through subprocess: {repo_new.remote(name='bot_remote').exists()=}",
        )

    origin = repo_new.remote(name="origin")
    origin.pull()

    while True:
        last_date_known = None
        last_date_processed = None
        last_hash_for_day = None

        pipeline = [{"$sort": {"height_for_last_block": -1}}, {"$limit": 1}]
        result = list(mongodb.mainnet[Collections.blocks_per_day].aggregate(pipeline))
        if len(result) == 1:
            last_date_known = result[0]["date"]
            last_hash_for_day = result[0]["hash_for_last_block"]

        result = mongodb.mainnet[Collections.helpers].find_one(
            {"_id": "last_known_nightly_accounts"}
        )
        if result:
            last_date_processed = result["date"]

        if last_date_known != last_date_processed:
            Daily(
                last_date_known,
                last_hash_for_day,
                grpcclient,
                repo_new,
                new_dir,
                mongodb,
                tooter,
            )
        else:
            console.log("Nothing to do...")
        time.sleep(5 * 60)
