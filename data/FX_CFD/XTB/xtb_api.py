from json import loads
from asyncio import get_event_loop

from websockets import connect

import settings


def Params(command, **kwargs):
    cmd =  """{{'command':'{0}','arguments':{1}}}""".format(command, kwargs).replace("'", '"')

    return cmd


async def jsnf(resp):
    return loads(resp)


async def Login():
    params = Params("login", userId=settings.USERID, password=settings.PASSWORD)

    async with connect(API) as ws:
        await ws.send(params)

        resp = await ws.recv()
        js = await jsnf(resp=resp)
        if js["status"]:
            print(">> Connection established")

            return js["streamSessionId"]


get_event_loop().run_until_complete(Login())