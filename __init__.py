import json
import pathlib
from typing import AsyncIterator

import discord
from discord import app_commands, Message

import breadcord
from .helpers.modified_internals import GuildScraperHelper


class Scraper(breadcord.module.ModuleCog):
    def __init__(self, module_id: str):
        super().__init__(module_id)
        self.scraped_guilds_path = self.module.storage_path / "scraped guilds"
        self.scraped_channels_path = self.module.storage_path / "scraped channels"

    async def scrape_guild(self, guild: discord.Guild, message_limit: int = 100):
        # TODO: Add support for scraping more guild data
        metadata = await self.bot.http.get_guild(guild.id, with_counts=True)
        metadata["members"] = [member async for member in GuildScraperHelper.fetch_members(guild, self.bot.http)]

        channels = []
        for channel in guild.channels:
            try:
                channels.append(await self.scrape_channel(channel, message_limit=message_limit))
            except discord.Forbidden:
                self.logger.warning(f"Can't access channel {channel.name} in guild {guild.name}")

        await self.save_guild_data(metadata=metadata, channels=channels)

    async def save_guild_data(
        self,
        *,
        metadata: dict,
        channels: list[tuple[dict, list[discord.Message, ...]], ...],
        root_dir: pathlib.Path = None,
    ) -> None:
        if root_dir is None:
            root_dir = self.scraped_guilds_path

        guild_path = root_dir / str(metadata["id"])
        guild_path.mkdir(parents=True,exist_ok=True)

        with open(guild_path / "metadata.json", "w") as f:
            json.dump(metadata, f, indent=4)

        for channel in channels:
            channel_path = guild_path
            channel_metadata = channel[0]
            channel_messages = channel[1]
            # If category
            if channel_metadata["type"] == 4:
                channel_path /= str(channel_metadata["id"])
                channel_path.mkdir(parents=True, exist_ok=True)
            # If the channel is in a category
            if channel_metadata["parent_id"] is not None:
                channel_path /= str(channel_metadata["parent_id"])

            channel_path.mkdir(parents=True, exist_ok=True)
            await self.save_channel_data(metadata=channel_metadata, messages=channel_messages, root_dir=channel_path)

    async def save_channel_data(
        self,
        *,
        metadata: dict,
        messages: list[discord.Message] | None,
        root_dir: pathlib.Path = None,
    ) -> None:
        if root_dir is None:
            root_dir = self.scraped_channels_path

        channel_path = root_dir / str(metadata["id"])
        channel_path.mkdir(exist_ok=True)

        with open(channel_path / "metadata.json", "w") as f:
            json.dump(metadata, f)
        if len(messages) > 0:
            with open(channel_path / "messages.json", "w") as f:
                json.dump(messages, f)

    async def fetch_channel_history(
        self, channel: discord.abc.Messageable, /, *, message_limit: int = 100
    ) -> AsyncIterator[discord.Message]:
        # Simplified version of
        # https://github.com/Rapptz/discord.py/blob/742630f1441d4b0b12a5fd9a751ab5cd1b39a5c6/discord/abc.py#L1647
        before = None
        while True:
            retrieve = min(100 if message_limit is None else message_limit, 100)
            if retrieve < 1:
                return

            before_id = before.id if before else None
            # noinspection PyUnresolvedReferences
            data = await self.bot.http.logs_from(channel.id, retrieve, before=before_id)

            if data:
                if message_limit is not None:
                    message_limit -= len(data)
                before = discord.Object(id=int(data[-1]["id"]))

            if len(data) < 100:
                message_limit = 0

            for raw_message in data:
                yield raw_message

    async def scrape_channel(
        self, channel: discord.abc.GuildChannel, *, message_limit: int = 100, save_at: pathlib.Path = None
    ) -> tuple[dict, list[Message | None]]:
        permissions = channel.permissions_for(channel.guild.me)
        messages = []
        if (
            isinstance(channel, discord.abc.Messageable)
            and permissions.view_channel
            # Fail if the channel is a voice channel and the bot can't connect
            and (not isinstance(channel, discord.VoiceChannel) or permissions.connect)
        ):
            self.logger.debug(f"Starting to scrape {channel.name}")
            messages = [message async for message in self.fetch_channel_history(channel, message_limit=message_limit)]
            self.logger.debug(f"Finished scraping {channel.name}")

        metadata = await self.bot.http.get_channel(channel.id)
        return metadata, messages

    @app_commands.command()
    async def scrape(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.GuildChannel | None = None,
        message_limit: int = None,
    ):
        if interaction.guild is None:
            return await interaction.response.send_message(
                "I sadly can't scrape DMs."
                "If you want to use the scrape command, you'll need to do so with a guild or a guild channel",
                ephemeral=True,
            )
        await interaction.response.defer()

        if channel is None:
            await self.scrape_guild(interaction.guild, message_limit=message_limit)
        else:
            try:
                metadata, messages = await self.scrape_channel(channel, message_limit=message_limit)
                await self.save_channel_data(metadata=metadata, messages=messages)
            except discord.Forbidden:
                return await interaction.followup.send(
                    "I don't have access to the channel you are trying to scrape!", ephemeral=True
                )

        await interaction.followup.send("Finished scraping")


async def setup(bot: breadcord.Bot):
    await bot.add_cog(Scraper("scraper"))
