import asyncio
import json
from datetime import datetime, timedelta

import websockets

from sqlalchemy import create_engine, select
from sqlalchemy import Column, Integer, Float, String, DateTime, Sequence
from sqlalchemy.orm import declarative_base, Session

import argparse

APIKEY_1 = '142159AF-1437-41AA-A29A-21D40A147911'
APIKEY_2 = '6B146CAF-2E7B-472C-8E9A-F33B89F0F69C'

Base = declarative_base()


class Transaction(Base):
    __tablename__ = 'transactions'
    id = Column(Integer, Sequence('id'), primary_key=True)
    time_exchange = Column(DateTime)
    time_coinapi = Column(DateTime)
    time_local = Column(DateTime)
    uuid = Column(String)
    price = Column(Float)
    size = Column(Float)
    taker_side = Column(String)
    symbol_id = Column(String)
    sequence = Column(Integer)
    type = Column(String)


async def collect_trades():
    engine = create_engine("sqlite:///test.db", echo=False, future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        async for trade, time_local in get_trades():
            skips = read_skips()
            if skips:
                skips -= 1
                write_skips(skips)
                continue
            trade = json.loads(trade)
            time_exchange = trade['time_exchange']
            datetime_time_exchange = time_coinapi_to_datetime(time_exchange)
            trade['time_exchange'] = datetime_time_exchange
            time_coinapi = trade['time_coinapi']
            datetime_time_coinapi = time_coinapi_to_datetime(time_coinapi)
            trade['time_coinapi'] = datetime_time_coinapi
            trade['time_local'] = time_local
            trade = Transaction(**trade)
            session.add(trade)
            session.commit()


async def test_validity(time_mode='coinapi'):
    async for volume_message, time_local in get_volume():
        volume_message = json.loads(volume_message)
        time_coinapi = volume_message["time_coinapi"]
        time_coinapi = time_coinapi_to_datetime(time_coinapi)
        if time_mode == 'local':
            datetime_to = time_local
        elif time_mode == 'coinapi':
            datetime_to = time_coinapi
        datetime_from = datetime_to - timedelta(hours=1)
        volume_db = await calculate_volume(datetime_from, datetime_to, time_mode)
        volume_by_symbol = volume_message["volume_by_symbol"]
        for symbol in volume_by_symbol:
            if symbol["symbol_id"] == "COINBASE_SPOT_BTC_USD":
                volume_coinapi = symbol["volume_base"]
        valid = volume_coinapi == volume_db
        if not valid:
            print(f"Volume {volume_coinapi} got from coinapi doesn't match {volume_db} calculated in database.")


async def fault_injection(skips):
    write_skips(skips + read_skips())


async def get_trades():
    uri = "wss://ws-sandbox.coinapi.io/v1/"
    async with websockets.connect(uri) as websocket:
        hello_message = {
            "type": "hello",
            "apikey": APIKEY_1,
            "heartbeat": False,
            "subscribe_data_type": ["trade"],
            "subscribe_filter_symbol_id": [
                "COINBASE_SPOT_BTC_USD"
            ]
        }
        await websocket.send(json.dumps(hello_message))
        while True:
            yield await websocket.recv(), datetime.now()


async def calculate_volume(datetime_from, datetime_to, time_mode):
    engine = create_engine("sqlite:///test.db", echo=False, future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        if time_mode == 'local':
            stmt = select(Transaction).where(Transaction.time_local.between(datetime_from, datetime_to))
        elif time_mode == 'coinapi':
            stmt = select(Transaction).where(Transaction.time_coinapi.between(datetime_from, datetime_to))
        sizes = [transaction.size for transaction in session.scalars(stmt)]
    volume = sum(sizes)
    return volume


async def get_volume():
    uri = "wss://ws-sandbox.coinapi.io/v1/"
    async with websockets.connect(uri) as websocket:
        hello_message = {
            "type": "hello",
            "apikey": APIKEY_2,
            "heartbeat": False,
            "subscribe_data_type": ["volume"],
            "subscribe_filter_period_id": ["1HRS"],
            "subscribe_filter_symbol_id": [
                "COINBASE_SPOT_BTC_USD"
            ]
        }
        await websocket.send(json.dumps(hello_message))
        while True:
            yield await websocket.recv(), datetime.now()


def time_coinapi_to_datetime(time_coinapi):
    return datetime.strptime(time_coinapi[:-2], '%Y-%m-%dT%H:%M:%S.%f')


def read_skips():
    with open('./skip_check.txt', 'r') as skip_check:
        return int(skip_check.read())


def write_skips(skips):
    with open('./skip_check.txt', 'w') as skip_check:
        skip_check.write(str(skips))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--collect', action='store_true')
    parser.add_argument('-t', '--test', type=str)
    parser.add_argument('-f', '--fault_injection', type=int)
    args = parser.parse_args()
    if args.collect:
        write_skips(0)
        asyncio.run(collect_trades())
    elif args.test:
        assert args.test in ['local', 'coinapi'], "-t has to be 'local' or 'coinapi'"
        asyncio.run(test_validity(args.test))
    elif args.fault_injection:
        assert isinstance(args.fault_injection, int), "-f has to be int"
        asyncio.run(fault_injection(args.fault_injection))
    else:
        print("One of flags -t or -v or -f is required")
