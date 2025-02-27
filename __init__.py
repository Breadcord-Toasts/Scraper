import asyncio
import contextlib
import json
import os
from collections.abc import Generator, Coroutine
from pathlib import Path
from typing import TypeVar, Sequence, cast, Protocol, Callable

import aiofiles
import discord
import discord.http
from discord import app_commands

import breadcord
from .helpers.modified_internals import fetch_channel_history, fetch_members, fetch_bans
from .helpers.output_parse import get_last_json_object

_T = TypeVar("_T")


class GuildMessageable(discord.abc.GuildChannel, discord.abc.Messageable):
    ...


class Identifiable(Protocol):
    id: int
    name: str


def logger_channel_reference(channel: Identifiable, /) -> str:
    return f'"{type(channel).__name__}" {channel.name} ({channel.id})'


def chunked(iterable: Sequence[_T], chunk_size: int, /) -> Generator[Sequence[_T], None, None]:
    for index in range(0, len(iterable), chunk_size):
        yield iterable[index:index + chunk_size]


async def gather_with_limit(
    *coros_or_futures: asyncio.Future[_T] | Coroutine[None, None, _T],
    limit: int,
    return_exceptions: bool = False
) -> list[_T | Exception]:
    """Operates like asyncio.gather() but never runs more than `limit` of the given coroutines at once."""
    pending = list(coros_or_futures)
    running = []
    results = []
    while pending or running:
        while len(running) < limit and pending:
            running.append(asyncio.ensure_future(pending.pop(0)))
        done, running = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            try:
                task.result()
            except Exception as error:
                if return_exceptions:
                    results.append(error)
                else:
                    raise error
        running = [task for task in running if not task.done()]
    return results


class Scraper(breadcord.helpers.HTTPModuleCog):
    def __init__(self, module_id: str, /) -> None:
        super().__init__(module_id)
        self.simultaneous_channels = 8

    async def scrape_channel(
        self,
        channel: discord.abc.GuildChannel,
        *,
        discord_logger: Callable[[str, bool], None],
        save_dir: Path,
        attachment_save_dir: Path | None = None,
        message_limit: int | None,
        include_threads: bool = False,
    ) -> None:
        save_dir.mkdir(parents=True, exist_ok=True)

        discord_logger(f"Scraping data from {channel.mention} `#{channel.name}`", False)
        await self.scrape_channel_metadata(channel, save_dir / f"{channel.id}_metadata.json")
        # Categories don't have messages
        if isinstance(channel, discord.CategoryChannel):
            return

        if (message_limit is None or message_limit > 0) and hasattr(channel, "history"):
            await self.scrape_channel_messages(
                # Above check establishes that the channel has a history method, meaning that it's messageable
                cast(GuildMessageable | discord.Thread, channel),
                message_save_path=save_dir / f"{channel.id}_messages.json",
                attachment_save_dir=attachment_save_dir,
                message_limit=message_limit,
            )
            discord_logger(f"Finished scraping messages in {channel.mention}", False)

        if isinstance(channel, discord.ForumChannel) or (include_threads and hasattr(channel, "threads")):
            all_threads = channel.threads  # type: ignore  # We checked with hasattr
            if hasattr(channel, "archived_threads"):
                with contextlib.suppress(discord.Forbidden):
                    async for thread in channel.archived_threads():  # type: ignore  # We checked with hasattr
                        if not any(thread.id == t.id for t in all_threads):
                            all_threads.append(thread)

            if all_threads:
                self.logger.debug(f"Scraping threads in {logger_channel_reference(channel)}...")
                discord_logger(f"Scraping threads in {channel.mention}", False)

            for thread in all_threads:
                thread_dir = save_dir / "threads" / str(thread.id)
                discord_logger(f"Scraping data from {thread.mention} `#{thread.name}`", False)

                await self.scrape_channel_metadata(thread, thread_dir / f"{thread.id}_metadata.json")
                if message_limit is None or message_limit > 0:
                    await self.scrape_channel_messages(
                        thread,
                        message_save_path=thread_dir / f"{thread.id}_messages.json",
                        attachment_save_dir=attachment_save_dir,
                        message_limit=message_limit,
                    )
            if all_threads:
                discord_logger(f"Finished scraping threads in {channel.mention}", False)

    async def scrape_channel_messages(
        self,
        channel: GuildMessageable | discord.Thread,
        /, *,
        message_save_path: Path,
        attachment_save_dir: Path | None = None,
        message_limit: int | None = 100
    ) -> None:
        self.logger.debug(f"Scraping messages in {logger_channel_reference(channel)}...")

        if self.session is None or self.session.closed:
            raise ValueError("Session is closed.")

        message_save_path.parent.mkdir(parents=True, exist_ok=True)

        permissions = channel.permissions_for(channel.guild.me)
        if not (permissions.read_message_history and permissions.read_messages):
            self.logger.debug(f"Skipping {logger_channel_reference(channel)} due to missing permissions.")
            return

        last_msg_id: int | None = None
        if message_save_path.is_file():
            # THis here code is evil
            try:
                last_msg_obj = await get_last_json_object(message_save_path)
            except Exception as error:
                self.logger.exception(
                    f"Failed to read last message in {logger_channel_reference(channel)}",
                    exc_info=error,  # IDE screams otherwise
                )
            else:
                last_msg_id: int | None = int(last_msg_id) if (last_msg_id := last_msg_obj.get("id")) else None
                if last_msg_id is None:
                    self.logger.error(f"Failed to get last message ID in {logger_channel_reference(channel)}")
                else:
                    self.logger.debug(
                        f"Found last message in {logger_channel_reference(channel)} (msg id: {last_msg_id}). "
                        "Preparing file for new insertions."
                    )
                    # Delete very last character ("]") to append a comma and start a new message
                    async with aiofiles.open(message_save_path, mode="r+", encoding="utf-8") as messages_file:
                        await messages_file.seek(0, os.SEEK_END)
                        await messages_file.seek((await messages_file.tell()) - 1)
                        await messages_file.truncate()

        try:
            async with (
                aiofiles.open(message_save_path, mode="w", encoding="utf-8")
                if last_msg_id is None
                else aiofiles.open(message_save_path, mode="a", encoding="utf-8")
            ) as messages_file:
                # If a channel has a lot of messages, we can get a MemoryError on machines without a lot of RAM
                # thus, we write each message one by one instead of saving them all in a list and then writing that
                if last_msg_id is None:
                    await messages_file.write("[")
                first_element = True
                async for message in fetch_channel_history(
                    channel,
                    message_limit=message_limit,
                    after_message_id=last_msg_id
                ):
                    if attachment_save_dir is not None:
                        attachment_save_dir.mkdir(parents=True, exist_ok=True)
                        for attachment in message.get("attachments", []):
                            if (url := attachment.get("proxy_url") or attachment.get("url")) is None:
                                continue
                            async with aiofiles.open(
                                attachment_save_dir / f"{attachment['id']} {attachment['filename']}",
                                mode="wb"
                            ) as attachment_file, self.session.get(url) as response:
                                await attachment_file.write(await response.read())
                    await messages_file.write(
                        ("" if first_element and not last_msg_id else ",")
                        + json.dumps(
                            message,
                            separators=(",", ":")  # Removes useless whitespace that'd increase file size
                        )
                    )
                    first_element = False
                await messages_file.write("]")
        except discord.HTTPException as error:
            message_save_path.unlink(missing_ok=True)
            self.logger.debug(f"Failed to scrape messages in {logger_channel_reference(channel)}: {error}")
        else:
            self.logger.debug(f"Finished scraping messages in {logger_channel_reference(channel)}.")

    async def scrape_channel_metadata(
        self,
        channel: discord.abc.GuildChannel | discord.Thread,
        save_path: Path,
    ) -> None:
        self.logger.debug(f"Scraping metadata of {logger_channel_reference(channel)}...")
        http = self.bot.http
        save_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            async with aiofiles.open(save_path, mode="w", encoding="utf-8") as file:
                channel_data = dict(await http.get_channel(channel.id))  # Type checker doesn't like TypedDict
                channel_data.update({
                    "pins": await http.pins_from(channel.id),
                })

                with contextlib.suppress(discord.HTTPException):
                    channel_data.update({"thread_members": await http.get_thread_members(channel.id)})
                with contextlib.suppress(discord.HTTPException):
                    channel_data.update({"webhooks": await http.channel_webhooks(channel.id)})
                with contextlib.suppress(discord.HTTPException):
                    channel_data.update({"invites": await http.invites_from_channel(channel.id)})

                await file.write(json.dumps(
                    channel_data,
                    separators=(",", ":")  # Removes useless whitespace
                ))
        except discord.HTTPException as error:
            save_path.unlink(missing_ok=True)
            self.logger.debug(f"Failed to scrape metadata of {logger_channel_reference(channel)}: {error}")
        else:
            self.logger.debug(f"Finished scraping metadata of {logger_channel_reference(channel)}.")

    async def scrape_guild_metadata(self, guild: discord.Guild, save_path: Path) -> None:
        self.logger.debug(f"Scraping metadata of guild {guild.name} ({guild.id}).")
        http = self.bot.http
        save_path.parent.mkdir(exist_ok=True, parents=True)
        async with aiofiles.open(save_path, mode="w", encoding="utf-8") as file:
            guild_data = dict(await http.get_guild(guild.id, with_counts=True))  # Type checker doesn't like TypedDict
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

            await file.write(json.dumps(
                guild_data,
                separators=(",", ":")  # Removes useless whitespace
            ))
        self.logger.debug(f"Finished scraping metadata of guild {guild.name} ({guild.id}).")

    @app_commands.command(description="Scrape the messages of an entire guild or a specific channel.")
    @app_commands.describe(
        channel="The channel to be scraped.",
        message_limit="How many messages should be scraped in each channel. Newest messages are scraped first.",
        notify_when_done="If the bot should ping you when it's done scraping.",
        scrape_threads="If the bot should scrape threads.",
        download_attachments="If the bot should download attachments. This will likely take up a lot of storage space.",
        verbose_logging="If the bot should log its progress in a thread rather than editing the original message.",
    )
    @app_commands.check(breadcord.helpers.administrator_check)
    async def scrape(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.GuildChannel | None = None,
        message_limit: int | None = None,
        notify_when_done: bool = False,
        scrape_threads: bool = True,
        download_attachments: bool = False,
        verbose_logging: bool = False,
    ) -> None:
        if not interaction.guild:
            await interaction.response.send_message("This command can only be used in a guild.", ephemeral=True)
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message(
                "This command can only be used in a text channel.",
                ephemeral=True,
            )
            return
        if verbose_logging and not interaction.channel.permissions_for(interaction.guild.me).create_public_threads:
            await interaction.response.send_message(
                "I need permission to create public threads in this channel to use verbose logging.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message("Requested scrape.")
        response_message: discord.Message = await interaction.channel.send("Scraping...")
        logging_thread: discord.Thread | None = None
        if verbose_logging:
            logging_thread = await response_message.create_thread(
                name="Scrape Log",
                reason="Thread for verbosely logging the progress of the ongoing scrape.",
            )

        status_pool: list[tuple[str, bool]] = []

        async def pool_report() -> bool:
            nonlocal status_pool
            status_pool = [(info, important) for info, important in status_pool if logging_thread or important]
            if not status_pool:
                return False

            if not logging_thread:
                await response_message.edit(content=status_pool[-1][0])
                status_pool.clear()
                return True

            while status_pool:
                joined = ""
                while status_pool:
                    info = status_pool[0][0]
                    if len(joined) + len(info) > 2000:
                        break
                    joined += info + "\n"
                    status_pool.pop(0)
                await logging_thread.send(joined)
            return True

        async def pool_checker() -> None:
            await asyncio.sleep(2)
            while True:
                if not status_pool:
                    await asyncio.sleep(3)
                    continue
                await pool_report()

        def add_to_pool(info: str, important: bool = True) -> None:
            status_pool.append((info, important))

        report_pool_task = asyncio.create_task(pool_checker())

        try:
            if channel is None:
                guild_path = self.module.storage_path / "guilds" / str(interaction.guild.id)
                guild_path.parent.mkdir(parents=True, exist_ok=True)

                add_to_pool("Scraping guild metadata...")
                await self.scrape_guild_metadata(interaction.guild, guild_path / "guild_metadata.json")

                add_to_pool("Scraping guild channels...")
                # Scrape multiple channels at once
                await gather_with_limit(
                    *(
                        self.scrape_channel(
                            channel=guild_channel,
                            discord_logger=add_to_pool,
                            save_dir=(save_dir := guild_path / str(guild_channel.id)),
                            attachment_save_dir=save_dir / "attachments" if download_attachments else None,
                            message_limit=message_limit,
                            include_threads=scrape_threads
                        )
                        for guild_channel in interaction.guild.channels
                    ),
                    limit=self.simultaneous_channels
                )
            else:
                channel_dir = self.module.storage_path / "channels" / str(channel.id)
                await self.scrape_channel(
                    channel=channel,
                    discord_logger=add_to_pool,
                    save_dir=channel_dir,
                    attachment_save_dir=channel_dir / "attachments" if download_attachments else None,
                    message_limit=message_limit,
                    include_threads=scrape_threads
                )

        except Exception:
            await response_message.edit(content="Scrape failed!")
            if logging_thread:
                await logging_thread.send("Scrape failed!")
                if not logging_thread.archived:
                    await logging_thread.edit(
                        name="Scrape Log (Failed)",
                        archived=(
                            interaction.channel.permissions_for(interaction.guild.me).manage_threads
                            or discord.utils.MISSING
                        ),
                    )
            if notify_when_done:
                await response_message.reply(content=f"{interaction.user.mention} Scrape failed!")
            raise
        else:
            await response_message.edit(content="Finished scraping.")
            if logging_thread:
                await logging_thread.send("Finished scraping.")
                if not logging_thread.archived:
                    await logging_thread.edit(
                        name="Scrape Log (Finished)",
                        archived=(
                            interaction.channel.permissions_for(interaction.guild.me).manage_threads
                            or discord.utils.MISSING
                        ),
                    )
            if notify_when_done:
                await response_message.reply(content=f"{interaction.user.mention} Scrape finished!")
        finally:
            report_pool_task.cancel()
            await pool_report()  # In case any stragglers are left


async def setup(bot: breadcord.Bot, module: breadcord.module.Module) -> None:
    await bot.add_cog(Scraper(module.id))
