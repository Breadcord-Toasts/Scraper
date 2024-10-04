import collections.abc
import io
import json
import os
import typing

import aiofiles
from aiofiles.threadpool.binary import AsyncBufferedReader

_T = typing.TypeVar("_T")
_R = typing.TypeVar("_R")

JSON_WHITESPACE = {" ", "\n", "\r", "\t"}


async def aenumerate(iterable: typing.AsyncIterable[_T], start: int = 0) -> typing.AsyncIterator[typing.Tuple[int, _T]]:
    """Equivalent to `enumerate` but for async iterables."""
    i = start
    async for item in iterable:
        yield i, item
        i += 1

async def amap(iterable: typing.AsyncIterable[_T], func: typing.Callable[[_T], _R]) -> typing.AsyncIterator[_R]:
    """Equivalent to `map` but for async iterables."""
    async for item in iterable:
        yield func(item)

async def to_aiter(iterable: collections.abc.Iterable[_T]) -> typing.AsyncIterator[_T]:
    """Converts a normal iterable to an async one."""
    for item in iterable:
        yield item

async def achain(*iterables: collections.abc.Iterable[_T] | typing.AsyncIterable[_T]) -> typing.AsyncIterator[_T]:
    """Chain together multiple iterables, async or not. First passed takes precedence."""
    for iterable in iterables:
        if isinstance(iterable, collections.abc.AsyncIterable):
            async for item in iterable:
                yield item
        else:
            for item in iterable:
                yield item


async def read_backwards(
    file_obj: AsyncBufferedReader,
    chunk_size: int = 1024,
    *,
    encoding: str = "utf-8",
) -> typing.AsyncIterator[str]:
    """Reads a file backwards in chunks of `chunk_size` bytes converted to a string. Chunk contents are NOT reversed."""
    steam_size = await file_obj.seek(0, io.SEEK_END)
    for _ in range(steam_size // chunk_size):
        await file_obj.seek(-chunk_size, io.SEEK_CUR)
        chunk = await file_obj.read(chunk_size)
        yield chunk.decode(encoding)
        await file_obj.seek(-chunk_size, io.SEEK_CUR)  # Undo the read

    await file_obj.seek(0)
    chunk = await file_obj.read(steam_size % chunk_size)
    yield chunk.decode(encoding)


async def get_last_json_object(
    file: str | os.PathLike[str],
    *,
    encoding: str = "utf-8",
) -> dict[str, typing.Any]:
    """Tries to get the last object from the JSON-file. Assumes that the file contains a list of objects."""
    async with aiofiles.open(file, "rb") as file_obj:
        read_iter = read_backwards(file_obj, encoding=encoding)
        read_iter = amap(read_iter, lambda c: c[::-1])

        chars: str = ""
        async for chunk in read_iter:
            found_end = chunk.find("]")
            if found_end == -1:
                continue
            if any(char not in JSON_WHITESPACE for char in chars + chunk[:found_end]):
                raise ValueError("Trailing characters found after list.")
            chars = chunk[found_end:]
            break
        if not chars:
            raise ValueError("No list was found in the file.")
        chars = chars[1:]  # Remove the list end char ("]"). It was only kept so the empty check above doesn't error

        extracted: str = ""
        square_nests: int = 0
        curly_nests: int = 0
        in_string: bool = False
        found_quote: bool = False
        should_exit: bool = False
        async for chunk in achain([chars], read_iter):
            if should_exit:
                break
            for char in chunk:
                extracted += char
                if char in JSON_WHITESPACE:
                    continue

                if not in_string:
                    if char == '"':
                        in_string = True
                else:
                    if char == '"':
                        found_quote = True
                    elif found_quote:
                        found_quote = False
                        if char != "\\":
                            in_string = False
                if in_string:
                    continue

                match char:
                    case '"':
                        in_string = True
                    # Technically these will count into the negatives... oh well, it doesn't change anything
                    case "[":
                        square_nests += 1
                    case "]":
                        square_nests -= 1
                    case "{":
                        curly_nests += 1
                    case "}":
                        curly_nests -= 1

                if curly_nests == 0:
                    should_exit = True
                    break

    return json.loads(extracted[::-1])
