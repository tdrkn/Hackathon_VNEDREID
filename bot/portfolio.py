"""Utility script to print a user's portfolio with a heuristic risk column."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

from tinkoff.invest import Client, InstrumentIdType
from tinkoff.invest.exceptions import UnauthenticatedError
from tinkoff.invest.schemas import Quotation
from tinkoff.invest.services import InstrumentsService

_RISK_STR = {0: "-", 1: "HIGH", 2: "MODERATE", 3: "LOW", 4: "MINIMAL"}

DEFAULT_RISK_BY_TYPE = {
    "currency": "LOW",
    "bond": "LOW",
    "etf": "MODERATE",
    "share": "MODERATE",
    "sp": "HIGH",
    "future": "HIGH",
}


TOKEN_ENV = "INVEST_TOKEN"
ACCOUNT_ENV = "INVEST_ACCOUNT_ID"


def q_to_float(q: Quotation) -> float:
    """Convert Quotation to float."""
    return q.units + q.nano / 1e9


def pick_token(token_opt: Optional[str]) -> str:
    token = (token_opt or os.getenv(TOKEN_ENV, "")).strip()
    if not token:
        sys.stderr.write("[ERROR] Specify Tinkoff token via --token or ENV.\n")
        raise SystemExit(1)
    return token


def make_resolver(instr: InstrumentsService):
    cache: Dict[str, Tuple[str, str, str]] = {}

    def call_type_specific(itype: str, uid: str):
        fn_name = f"get_{itype}_by"
        fn = getattr(instr, fn_name, None)
        if not callable(fn):
            return None
        try:
            return fn(id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_UID, id=uid).instrument
        except Exception:
            return None

    def resolve(uid: str, figi: str, itype: str, currency: str):
        if uid in cache:
            return cache[uid]

        itype_l = itype.lower()
        if itype_l == "currency":
            res = (currency.upper(), currency.upper(), "LOW")
            cache[uid] = res
            return res

        data = call_type_specific(itype_l, uid)
        if not data:
            try:
                data = instr.get_instrument_by(id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_UID, id=uid).instrument
            except Exception:
                data = None

        if data:
            ticker, name = data.ticker, data.name
            risk_num = int(getattr(data, "risk_level", 0))
            risk = _RISK_STR.get(risk_num, "-")
            if risk == "-":
                risk = DEFAULT_RISK_BY_TYPE.get(itype_l, "MODERATE")
        else:
            ticker, name, risk = "-", "Unknown instrument", DEFAULT_RISK_BY_TYPE.get(itype_l, "MODERATE")

        cache[uid] = (ticker, name, risk)
        return ticker, name, risk

    return resolve


def main() -> None:
    parser = argparse.ArgumentParser("Show portfolio with heuristic risk level")
    parser.add_argument("--token", required=False)
    parser.add_argument("-a", "--account", help="Account ID")
    args = parser.parse_args()

    token = pick_token(args.token)

    try:
        with Client(token=token) as cli:
            accounts = cli.users.get_accounts().accounts
    except UnauthenticatedError:
        sys.stderr.write("[AUTH] Токен отклонён.\n")
        raise SystemExit(1)

    if not accounts:
        print("Нет счетов.")
        return

    acc_id = args.account or os.getenv(ACCOUNT_ENV) or accounts[0].id

    with Client(token=token) as cli:
        positions = cli.operations.get_portfolio(account_id=acc_id).positions
        resolve = make_resolver(cli.instruments)

        print(
            f"\nPortfolio for account {acc_id} — {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}"
        )
        header = (
            f"{'FIGI':<12} {'Ticker':<8} {'Name':<30} {'Qty':>10} {'Curr':<6} "
            f"{'Price':>14} {'Value':>14} {'Risk':<8}"
        )
        print(header)
        print("-" * len(header))

        for p in positions:
            figi = p.figi
            qty = q_to_float(p.quantity)
            curr = p.average_position_price.currency or "-"
            price = q_to_float(p.current_price)
            value = price * qty
            ticker, name, risk = resolve(p.instrument_uid, figi, p.instrument_type, curr)
            print(
                f"{figi:<12} {ticker:<8} {name:<30} {qty:10,.3f} {curr:<6} {price:14,.2f} {value:14,.2f} {risk:<8}"
            )


if __name__ == "__main__":
    main()
