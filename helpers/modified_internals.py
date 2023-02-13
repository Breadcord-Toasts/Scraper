import datetime
from typing import AsyncIterator

import discord
from discord.http import HTTPClient


class GuildScraperHelper:
    @staticmethod
    async def fetch_members(guild: discord.Guild, http: HTTPClient) -> AsyncIterator[dict]:
        # Slightly edited version of the discord.Guild.fetch_members method
        limit = None
        after = discord.utils.MISSING

        while True:
            retrieve = min(1000 if limit is None else limit, 1000)
            if retrieve < 1:
                return

            if isinstance(after, datetime.datetime):
                after = discord.Object(id=discord.utils.time_snowflake(after, high=True))

            after = after or discord.object.OLDEST_OBJECT
            after_id = after.id if after else None

            data = await http.get_members(guild.id, retrieve, after_id)
            if not data:
                return

            after = discord.object.Object(id=int(data[-1]['user']['id']))

            if len(data) < 1000:
                limit = 0

            for raw_member in reversed(data):
                yield raw_member

