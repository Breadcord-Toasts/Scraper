import contextlib
import json
from pathlib import Path

import aiofiles
import discord
import discord.http
from discord import app_commands

import breadcord
from .helpers.modified_internals import fetch_channel_history, fetch_members, fetch_bans


def logger_channel_reference(channel: discord.abc.Messageable | discord.abc.GuildChannel, /) -> str:
    return f"{type(channel).__name__} {channel.name} ({channel.id})"


class GuildMessageable(discord.abc.GuildChannel, discord.abc.Messageable):
    ...


class Scraper(breadcord.module.ModuleCog):
    def __init__(self, module_id: str, /):
        super().__init__(module_id)

    async def scrape_channel(
        self,
        channel: discord.abc.GuildChannel,
        /,
        save_dir: Path,
        *,
        message_limit: int | None,
        include_threads: bool = False
    ) -> None:
        save_dir.mkdir(parents=True, exist_ok=True)

        await self.scrape_channel_metadata(channel, save_dir / f"{channel.id}_metadata.json")
        if (message_limit is None or message_limit > 0) and hasattr(channel, "history"):
            channel: GuildMessageable
            await self.scrape_channel_messages(
                channel,
                save_dir / f"{channel.id}_messages.json",
                message_limit=message_limit
            )

        if include_threads and hasattr(channel, "threads"):
            for thread in channel.threads:
                thread: discord.Thread
                thread_dir = save_dir / "threads" / str(thread.id)

                await self.scrape_channel_metadata(thread, thread_dir / f"{thread.id}_metadata.json")
                if message_limit is None or message_limit > 0:
                    await self.scrape_channel_messages(
                        thread,
                        thread_dir / f"{thread.id}_messages.json",
                        message_limit=message_limit
                    )

    async def scrape_channel_messages(
        self, channel: discord.abc.Messageable, /, save_path: Path, *, message_limit: int | None = 100
    ) -> None:
        self.logger.debug(f"Scraping messages in {logger_channel_reference(channel)}...")
        save_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            async with aiofiles.open(save_path, mode="w", encoding="utf-8") as file:
                # If a channel has a lot of messages, we can get a MemoryError on machines without a lot of RAM
                # thus, we write each message one by one instead of them all in a list and then writing that
                await file.write("[")
                first_element = True
                async for message in fetch_channel_history(channel, message_limit=message_limit):
                    await file.write(("" if first_element else ",") + json.dumps(message))
                    first_element = False
                await file.write("]")
        except discord.HTTPException as error:
            save_path.unlink(missing_ok=True)
            self.logger.debug(f"Failed to scrape messages in {logger_channel_reference(channel)}: {error}")
        else:
            self.logger.debug(f"Finished scraping messages in {logger_channel_reference(channel)}.")

    async def scrape_channel_metadata(
        self, channel: discord.abc.GuildChannel | discord.Thread, /, save_path: Path
    ) -> None:
        self.logger.debug(f"Scraping metadata of {logger_channel_reference(channel)}...")
        http = self.bot.http
        save_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            async with aiofiles.open(save_path, mode="w", encoding="utf-8") as file:
                channel_data = await http.get_channel(channel.id)
                channel_data.update({
                    "pins": await http.pins_from(channel.id),
                })

                with contextlib.suppress(discord.HTTPException):
                    channel_data.update({"thread_members": await http.get_thread_members(channel.id)})
                with contextlib.suppress(discord.HTTPException):
                    channel_data.update({"webhooks": await http.channel_webhooks(channel.id)})
                with contextlib.suppress(discord.HTTPException):
                    channel_data.update({"invites": await http.invites_from_channel(channel.id)})

                await file.write(json.dumps(channel_data))
        except discord.HTTPException as error:
            save_path.unlink(missing_ok=True)
            self.logger.debug(f"Failed to scrape metadata of {logger_channel_reference(channel)}: {error}")
        else:
            self.logger.debug(f"Finished scraping metadata of {logger_channel_reference(channel)}.")

    async def scrape_guild_metadata(
        self, guild: discord.Guild, /, save_path: Path
    ) -> None:
        self.logger.debug(f"Scraping metadata of guild {guild.name} ({guild.id}).")
        http = self.bot.http
        save_path.parent.mkdir(exist_ok=True, parents=True)
        async with aiofiles.open(save_path, mode="w", encoding="utf-8") as file:
            guild_data = await http.get_guild(guild.id, with_counts=True)
            guild_data.update({
                "channels": await http.get_all_guild_channels(guild.id),
                "active_threads": await http.get_active_threads(guild.id),
                "onboarding": await http.request(discord.http.Route(
                    'GET', f'/guilds/{guild.id}/onboarding',
                    guild_id=guild.id
                )),
                "scheduled_events": await http.get_scheduled_events(guild.id, with_user_count=True),
            })

            with contextlib.suppress(discord.Forbidden):
                # Locked behind GUILD_MEMBERS intent
                guild_data.update({"members": [member async for member in fetch_members(guild, limit=None)]})
            with contextlib.suppress(discord.Forbidden):
                guild_data.update({"bans": [ban async for ban in fetch_bans(guild, limit=None)]})
            with contextlib.suppress(discord.Forbidden):
                # These all require the MANAGE_GUILD permission
                guild_data.update({
                    "invites": await http.invites_from(guild.id),
                    "vanity_url": await http.get_vanity_code(guild.id),
                    "welcome_screen": await http.get_welcome_screen(guild.id),
                    # Limited to 50 integrations
                    # https://discord.com/developers/docs/resources/guild#get-guild-integrations
                    "integrations": await http.get_all_integrations(guild.id)
                })
            with contextlib.suppress(discord.Forbidden):
                guild_data.update({"widget": await http.get_widget(guild.id)})

            await file.write(json.dumps(guild_data))
        self.logger.debug(f"Finished scraping metadata of guild {guild.name} ({guild.id}).")

    @app_commands.command(description="Scrape the messages of an entire guild or a specific channel.")
    @app_commands.describe(
        channel="The channel to be scraped.",
        message_limit="How many messages should be scraped in each channel. Newest messages are scraped first.",
        notify_when_done="If the bot should ping you when it's done scraping.",
        scrape_threads="If the bot should scrape threads.",
    )
    @app_commands.check(breadcord.helpers.administrator_check)
    async def scrape(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.GuildChannel | None = None,
        message_limit: int | None = None,
        notify_when_done: bool = False,
        scrape_threads: bool = True
    ):
        if not interaction.guild:
            await interaction.response.send_message("This command can only be used in a guild.", ephemeral=True)
            return

        try:
            message: discord.Message = await interaction.channel.send("Scraping...")
        except discord.Forbidden:
            await interaction.response.send_message(
                "Aborting scrape.\n"
                "Could not send message in this channel. Please make sure the bot has the correct permissions.",
                ephemeral=True
            )
            return
        await interaction.response.send_message("Starting to scrape.", ephemeral=True)

        if channel is None:
            guild_path = self.module.storage_path / "guilds" / str(interaction.guild.id)
            guild_path.parent.mkdir(parents=True, exist_ok=True)

            await message.edit(content="Scraping guild metadata...")
            await self.scrape_guild_metadata(interaction.guild, guild_path / "guild_metadata.json")

            for index, guild_channel in enumerate(channels := interaction.guild.channels):
                await message.edit(content=f"Scraping {index+1}/{len(channels)} channels...")
                await self.scrape_channel(
                    guild_channel,
                    guild_path / str(guild_channel.id),
                    message_limit=message_limit,
                    include_threads=scrape_threads
                )
        else:
            await message.edit(content="Scraping channel...")
            await self.scrape_channel(
                channel,
                self.module.storage_path / "channels" / str(channel.id),
                message_limit=message_limit,
                include_threads=scrape_threads
            )
        await message.edit(content="Finished scraping.")

        if notify_when_done:
            await message.reply(interaction.user.mention)


async def setup(bot: breadcord.Bot):
    await bot.add_cog(Scraper("scraper"))
