import strawberry
import motor.motor_asyncio
import decimal
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from strawberry.asgi import GraphQL
from typing import List, Optional
from aio_pika import connect_robust, Message, DeliveryMode
import aioredis
from bson import ObjectId


from src.models import Tx, User

cli = motor.motor_asyncio.AsyncIOMotorClient('localhost', 27017)

db = cli.yat
tx_collection = db.txs
users_collection = db.users
asks = db.asks
votes = db.votes

@strawberry.experimental.pydantic.type(model=User, all_fields=True)
class GraphUser:
    addr: str
    name: Optional[str]
    cover: Optional[str]
    desc: Optional[str]
    sign: str


@strawberry.experimental.pydantic.type(model=Tx)
class GraphTx:
    credit: Optional[str]
    debit: Optional[str]
    amount: Optional[float]
    time: Optional[decimal.Decimal]
    sign: Optional[str]
    hash: strawberry.auto
    msg: strawberry.auto
    
    
async def fetch_all_users():
    users = await users_collection.find().to_list(None)
    graph_users = [
        GraphUser(
            addr=user["addr"],
            name=user["name"],
            cover=user["cover"],
            desc=user["desc"],
            sign=user["sign"]
        )
        for user in users
    ]
    return graph_users


async def fetch_all_tx():
    txs = await tx_collection.find().to_list(None)
    graph_tx = [
        GraphTx(
            credit=tx["credit"],
            debit=tx["debit"],
            amount=tx["amount"],
            time=tx["time"],
            sign=tx["sign"],
            hash=tx["hash"],
            msg=tx["msg"]
            
        )
        for tx in txs
    ]
    return graph_tx

async def fetch_byid_tx(tx_id):
    obj_id = ObjectId(tx_id)
    txs = await tx_collection.find_one({'_id': obj_id})
    print(txs)
    graph_tx = GraphTx(
        credit=txs.get("credit"),
        debit=txs.get("debit"),
        amount=txs.get("amount"),
        time=txs.get("time"),
        sign=txs.get("sign"),
        hash=txs.get("hash")
        )
    return graph_tx 

async def fetch_byuser_tx(addr):
    txs_cr = await tx_collection.find({'credit': addr}).to_list(None)
    txs_db = await tx_collection.find({'debit': addr}).to_list(None)
    txs_cr.extend(txs_db)
    graph_tx = []
    if txs_cr:
        graph_tx = [
            GraphTx(
                credit=tx["credit"],
                debit=tx["debit"],
                amount=tx["amount"],
                time=tx["time"],
                sign=tx["sign"],
                hash=tx["hash"],
                msg=tx["msg"]
                
            )
            for tx in txs_cr
        ]
    return graph_tx

async def fetch_all_user_contacts(addr):
    
    txs_cr = await tx_collection.find({'credit': addr}).to_list(None)
    txs_db = await tx_collection.find({'debit': addr}).to_list(None)
    all_users = []
    
    for tx in txs_cr:
        debit = tx.get("debit")
        
        users = await users_collection.find({'addr': debit}).to_list(None)
        
        if users:
            all_users.extend(users)
    for tx in txs_db:
        credit = tx.get("credit")
        
        users = await users_collection.find({'addr': credit}).to_list(None)
        
        if users:
            all_users.extend(users)
    
    graph_users = [
        GraphUser(
            addr=user["addr"],
            name=user["name"],
            cover=user["cover"],
            desc=user["desc"],
            sign=user["sign"]
        )
        for user in all_users
    ]
    print(txs_cr)
    return graph_users


async def load_balance(addr):
    #ok = await redis.set("key", "value")
    #assert ok
    #b = await redis.get(addr)
    #print(int(b))
    #return int(b)
    txs_cr = await tx_collection.find({'credit': addr}).to_list(None)
    txs_db = await tx_collection.find({'debit': addr}).to_list(None)
    total = 0
    for tx in txs_db:
        total = total +tx.get("amount")
    for tx in txs_cr:
        total = total -tx.get("amount")
    return total

async def load_user(addr, name, cover, desc, sign)->List[GraphUser]:
    query = {}
    if addr:
        query['addr'] = addr
    if name:
        query['name'] = name
    if cover:
        query['cover'] = cover
    if desc:
        query['desc'] = desc
    if sign:
        query['sign'] = sign
    
    users = await users_collection.find(query).to_list(None)
    graph_users = [
        GraphUser(
            addr=user["addr"],
            name=user["name"],
            cover=user["cover"],
            desc=user["desc"],
            sign=user["sign"]
        )
        for user in users
    ]
    return graph_users



async def load_tx(amount, addr, msg, time, skip, limit) -> List[GraphTx]:
    query = {}
    if amount is not 0:
        query['amount'] = amount
    if addr:
        query['$or'] = [{'credit': addr}, {'debit': addr}]
    if msg is not '':
        query['msg'] = msg
    if time is not 0:
        query['time'] = time
    
    txs = await tx_collection.find(query).skip(skip).limit(limit).to_list(None)
    graph_txs = [
        GraphTx(
            credit=tx["credit"],
            debit=tx["debit"],
            amount=tx["amount"],
            time=tx["time"],
            sign=tx["sign"],
            hash=tx.get("hash"),
            msg=tx.get("msg")
        )
        for tx in txs
    ]
    
    return graph_txs

@strawberry.type
class Query:
    @strawberry.field
    async def tx(
        self,
        msg: str = '',
        addr: str = '',
        amount: int = 0,
        time: int = 0,
        skip: int = 0,
        limit: int = 100
    ) -> List[GraphTx]:
        return await load_tx(amount, addr, msg, time, skip, limit)
    
    @strawberry.field
    async def user(
        self,
        addr: str = '',
        name: str = '',
        cover: str = '',
        desc: str = '',
        sign: str = ''
    ) -> List[GraphUser]:
        return await load_user(addr, name, cover, desc, sign)

    @strawberry.field
    async def balance(
        self,
        addr: str = ''
    ) -> float:
        return await load_balance(addr)

    @strawberry.field
    async def get_all_users(self) -> List[GraphUser]:
        return await fetch_all_users()

    @strawberry.field
    async def get_all_TX(self) -> List[GraphTx]:
        return await fetch_all_tx()

    @strawberry.field
    async def get_TX_by_id(self, _id: str) -> GraphTx:
        return await fetch_byid_tx(_id)

    @strawberry.field
    async def get_TX_by_user(self, addr: str) -> List[GraphTx]:
        return await fetch_byuser_tx(addr)

    @strawberry.field()
    async def get_all_user_contacts(self, addr: str) -> List[GraphUser]:
        return await fetch_all_user_contacts(addr)


schema = strawberry.Schema(query=Query)


graphql_app = GraphQL(schema)#graphiql=False

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup() -> None:
    global mq
    connection = await connect_robust("amqp://guest:guest@localhost/")
    mq = await connection.channel()
    queue = await mq.declare_queue("tx", durable=True)
    global redis
    redis = await aioredis.from_url("redis://localhost",  db=0)

app.add_route("/graphql", graphql_app)
app.add_websocket_route("/graphql", graphql_app)
