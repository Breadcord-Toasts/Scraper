import datetime
from collections.abc import AsyncIterator
from typing import Union

import discord


async def fetch_channel_history(
    channel: discord.abc.GuildChannel | discord.Thread,
    /, *,
    message_limit: int | None = 100
) -> AsyncIterator[dict]:
    # Simplified version of
    # https://github.com/Rapptz/discord.py/blob/742630f1441d4b0b12a5fd9a751ab5cd1b39a5c6/discord/abc.py#L1647
    # using only the "after" strategy

    after_message = discord.Object(id=0)
    while True:
        retrieve_count = 100 if message_limit is None else min(message_limit, 100)
        if retrieve_count < 1:
            return

        after_id = after_message.id if after_message else None
        # noinspection PyProtectedMember
        data = await channel._state.http.logs_from(channel.id, retrieve_count, after=after_id)
        if data:
            if message_limit is not None:
                message_limit -= len(data)
            after_message = discord.Object(id=int(data[0]['id']))

        data = reversed(data)
        count = 0
        for count, raw_message in enumerate(data, 1):
            yield raw_message

        if count < 100:
            break


async def fetch_members(
    guild: discord.Guild,
    /, *,
    limit: int | None = 1000,
    after: Union[discord.abc.Snowflake, datetime] = discord.utils.MISSING
) -> AsyncIterator[dict]:
    # Modified to not yield discord.Member objects, but instead just the raw data
    # https://github.com/Rapptz/discord.py/blob/16f1760dd08a91649f594799fff4c39bdf52c0ac/discord/guild.py#L2225

    # noinspection PyProtectedMember
    if not guild._state.intents.members:
        raise discord.ClientException("Intents.members must be enabled to use this.")

    while True:
        retrieve = 1000 if limit is None else min(limit, 1000)
        if retrieve < 1:
            return

        if isinstance(after, datetime.datetime):
            after = discord.Object(id=discord.utils.time_snowflake(after, high=True))

        after = after or discord.abc.OLDEST_OBJECT
        after_id = after.id if after else None
        # noinspection PyProtectedMember
        state = guild._state

        data = await state.http.get_members(guild.id, retrieve, after_id)
        if not data:
            return

        # Terminate loop on next iteration; there's no data left after this
        if len(data) < 1000:
            limit = 0

        after = discord.Object(id=int(data[-1]["user"]["id"]))

        for raw_member in reversed(data):
            yield raw_member


async def fetch_bans(guild: discord.Guild, /, *, limit: int | None = 1000) -> AsyncIterator[dict]:
    # Simplified version of
    # https://github.com/Rapptz/discord.py/blob/16f1760dd08a91649f594799fff4c39bdf52c0ac/discord/guild.py#L2225

    # This endpoint paginates in ascending order.
    # noinspection PyProtectedMember
    endpoint = guild._state.http.get_bans

    async def _after_strategy(retrieve: int, after: discord.abc.Snowflake | None, limit: int | None):
        after_id = after.id if after else None
        data = await endpoint(guild.id, limit=retrieve, after=after_id)

        if data:
            if limit is not None:
                limit -= len(data)

            after = discord.Object(id=int(data[-1]['user']['id']))

        return data, after, limit

    state = discord.utils.MISSING
    while True:
        retrieve = 1000 if limit is None else min(limit, 1000)
        if retrieve < 1:
            return

        data, state, limit = await _after_strategy(retrieve, state, limit)

        # Terminate loop on next iteration; there's no data left after this
        if len(data) < 1000:
            limit = 0

        for ban in data:
            yield ban



