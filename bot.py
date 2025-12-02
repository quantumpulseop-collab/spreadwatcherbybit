#!/usr/bin/env python3
import time
import requests
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import traceback

# ============================= CONFIG =============================
TELEGRAM_TOKEN = "8558387224:AAElr4m5BYIsquJBOr0PBh1-OXZ4dzMrer4"
TELEGRAM_CHAT_IDS = ["5054484162", "497819952"]

SCAN_THRESHOLD = 0.25       # Min % to shortlist candidates
ALERT_THRESHOLD = 5.0       # Instant alert threshold in %
ALERT_COOLDOWN = 60         # seconds - cooldown per symbol
SUMMARY_INTERVAL = 300      # not used in minute-window design but kept
MAX_WORKERS = 12

MONITOR_DURATION = 60       # seconds per monitoring window (1 minute)
MONITOR_POLL = 2            # seconds between polls during the monitoring window
CONFIRM_RETRY_DELAY = 0.5   # seconds between initial detection and confirm re-check
CONFIRM_RETRIES = 2         # how many confirm rechecks to do (fast, to reduce false positives)
# ==================================================================

# API endpoints
BINANCE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_BOOK_URL = "https://fapi.binance.com/fapi/v1/ticker/bookTicker"
BINANCE_TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol={symbol}"
GATEIO_INFO_URL = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
GATEIO_TICKER_URL = "https://api.gateio.ws/api/v4/futures/usdt/tickers"

# -------------------- Logging setup --------------------
logger = logging.getLogger("arb_monitor")
logger.setLevel(logging.DEBUG)

# Console handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
ch.setFormatter(ch_formatter)
logger.addHandler(ch)

# Rotating file handler for detailed debug logs
fh = RotatingFileHandler("arb_bot.log", maxBytes=5_000_000, backupCount=5)
fh.setLevel(logging.DEBUG)
fh_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
fh.setFormatter(fh_formatter)
logger.addHandler(fh)

def timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# -------------------- Telegram helper --------------------
def send_telegram(message):
    for chat_id in TELEGRAM_CHAT_IDS:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            resp = requests.get(url, params={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            }, timeout=10)
            if resp.status_code != 200:
                logger.warning("Telegram non-200 response: %s %s", resp.status_code, resp.text[:200])
        except Exception:
            logger.exception("Failed to send Telegram message")

# -------------------- Utility / fetch functions --------------------
def normalize(sym):
    """Normalize symbol names to a common comparable form."""
    if not sym:
        return sym
    s = sym.upper()
    # Gate.io contracts: BTC_USDT, strip underscore for comparison with Binance BTCUSDT
    s = s.replace('_', '')
    return s

def get_binance_symbols(retries=2):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(BINANCE_INFO_URL, timeout=10)
            r.raise_for_status()
            data = r.json()
            syms = [s["symbol"] for s in data.get("symbols", [])
                    if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING"]
            logger.debug("[BINANCE] fetched %d symbols (sample: %s)", len(syms), syms[:6])
            return syms
        except Exception as e:
            logger.warning("[BINANCE] attempt %d error: %s", attempt, str(e))
            if attempt == retries:
                logger.exception("[BINANCE] final failure fetching symbols")
                return []
            time.sleep(0.7)

def get_gateio_symbols(retries=2):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(GATEIO_INFO_URL, timeout=10)
            r.raise_for_status()
            data = r.json()
            syms = [s["name"] for s in data if s.get("in_delisting") is False and s.get("is_open") is True]
            logger.debug("[GATEIO] fetched %d symbols (sample: %s)", len(syms), syms[:6])
            return syms
        except Exception as e:
            logger.warning("[GATEIO] attempt %d error: %s", attempt, str(e))
            if attempt == retries:
                logger.exception("[GATEIO] final failure fetching symbols")
                return []
            time.sleep(0.7)

def get_common_symbols():
    bin_syms = get_binance_symbols()
    gate_syms = get_gateio_symbols()
    bin_set = {normalize(s) for s in bin_syms}
    gate_set = {normalize(s) for s in gate_syms}
    common = bin_set.intersection(gate_set)
    gate_map = {}
    dup_count = 0
    for s in gate_syms:
        n = normalize(s)
        if n in gate_map and gate_map[n] != s:
            dup_count += 1
            # keep first and warn in debug
        else:
            gate_map[n] = s
    if dup_count:
        logger.warning("Duplicate normalized Gate.io symbols detected: %d (kept first)", dup_count)
    logger.info("Common symbols: %d (sample: %s)", len(common), list(common)[:8])
    return common, gate_map

def get_binance_book(retries=1):
    """Fetch full binance bookTicker list and return dict symbol->{bid,ask}"""
    for attempt in range(1, retries+1):
        try:
            r = requests.get(BINANCE_BOOK_URL, timeout=10)
            r.raise_for_status()
            data = r.json()
            out = {}
            for d in data:
                try:
                    out[d["symbol"]] = {"bid": float(d["bidPrice"]), "ask": float(d["askPrice"])}
                except Exception:
                    continue
            logger.debug("[BINANCE_BOOK] entries: %d", len(out))
            return out
        except Exception:
            logger.exception("[BINANCE_BOOK] fetch error")
            if attempt == retries:
                return {}
            time.sleep(0.5)

def get_binance_price(symbol, session, retries=1):
    """Fetch single-symbol binance bookTicker"""
    for attempt in range(1, retries+1):
        try:
            url = BINANCE_TICKER_URL.format(symbol=symbol)
            r = session.get(url, timeout=6)
            if r.status_code != 200:
                logger.debug("Binance ticker non-200 %s for %s: %s", r.status_code, symbol, r.text[:200])
                return None, None
            d = r.json()
            bid = float(d.get("bidPrice") or 0)
            ask = float(d.get("askPrice") or 0)
            if bid <= 0 or ask <= 0:
                return None, None
            return bid, ask
        except Exception:
            logger.debug("Binance price fetch failed for %s (attempt %d)", symbol, attempt)
            if attempt == retries:
                logger.exception("Binance price final failure for %s", symbol)
                return None, None
            time.sleep(0.2)

def get_gateio_tickermap():
    """Fetch all Gate.io USDT perpetual tickers, return symbol->ticker dict"""
    try:
        r = requests.get(GATEIO_TICKER_URL, timeout=10)
        r.raise_for_status()
        data = r.json()
        out = {}
        for d in data:
            sym = d["contract"]
            out[sym] = d
        logger.debug("[GATEIO_TICKERS] entries: %d", len(out))
        return out
    except Exception:
        logger.exception("[GATEIO_TICKERS] fetch error")
        return {}

def get_gateio_price_once(symbol, session, retries=1):
    """Fetch single-symbol Gate.io USDT futures ticker (perpetual)"""
    # Gate.io uses all-tickers endpoint, so we use session-less, ignore retries here
    try:
        tickers = get_gateio_tickermap()
        if symbol not in tickers:
            return None, None
        d = tickers[symbol]
        bid = float(d.get("bid1", 0))
        ask = float(d.get("ask1", 0))
        if bid <= 0 or ask <= 0:
            return None, None
        return bid, ask
    except Exception:
        logger.debug("Gate.io price fetch failed for %s", symbol)
        return None, None

def threaded_gateio_prices(symbols):
    """Parallel fetch of Gate.io prices for a list of symbols (original Gate.io symbol forms)"""
    prices = {}
    if not symbols:
        return prices
    # fetch shared ticker map ONCE
    tickers = get_gateio_tickermap()
    for s in symbols:
        try:
            if s in tickers:
                bid = float(tickers[s].get("bid1", 0))
                ask = float(tickers[s].get("ask1", 0))
                if bid and ask:
                    prices[s] = {"bid": bid, "ask": ask}
        except Exception:
            logger.exception("threaded_gateio_prices: error for %s", s)
    logger.debug("[GATEIO_BATCH] fetched %d/%d", len(prices), len(symbols))
    return prices

# -------------------- Spread calculation --------------------
def calculate_spread(bin_bid, bin_ask, gate_bid, gate_ask):
    """Return positive spread when Gate.io bid > Binance ask (long bin, short gate),
       negative spread when Gate.io ask < Binance bid (long gate, short bin)"""
    try:
        if not all([bin_bid, bin_ask, gate_bid, gate_ask]) or bin_ask <= 0 or bin_bid <= 0:
            return None
        pos = ((gate_bid - bin_ask) / bin_ask) * 100
        neg = ((gate_ask - bin_bid) / bin_bid) * 100
        if pos > 0.01:
            return pos
        if neg < -0.01:
            return neg
        return None
    except Exception:
        logger.exception("calculate_spread error")
        return None

# -------------------- Main loop (1-min windows + focused monitoring) --------------------
def main():
    logger.info("Binance ↔ Gate.io Monitor STARTED - %s", timestamp())
    send_telegram("Bot started — 1-min windows. Full scan each minute; watch shortlisted coins for that minute. Instant alerts on ±5%")

    last_alert = {}
    heartbeat_counter = 0
    http_session = requests.Session()

    while True:
        window_start = time.time()
        try:
            # 1) Full scan once at start of window
            common_symbols, gate_map = get_common_symbols()
            if not common_symbols:
                logger.warning("No common symbols — retrying after short sleep")
                time.sleep(5)
                continue

            bin_book = get_binance_book()
            gate_symbols = [gate_map.get(sym, f"{sym}_USDT") for sym in common_symbols]
            gate_prices = threaded_gateio_prices(gate_symbols)

            candidates = {}
            for sym in common_symbols:
                bin_tick = bin_book.get(sym)
                gate_sym = gate_map.get(sym, f"{sym}_USDT")
                gate_tick = gate_prices.get(gate_sym)
                if not bin_tick or not gate_tick:
                    continue
                spread = calculate_spread(bin_tick["bid"], bin_tick["ask"], gate_tick["bid"], gate_tick["ask"])
                if spread is not None and abs(spread) >= SCAN_THRESHOLD:
                    candidates[sym] = {
                        "gate_sym": gate_sym,
                        "start_spread": spread,
                        "max_spread": spread,
                        "min_spread": spread,
                        "alerted": False
                    }

            logger.info("[%s] Start window: shortlisted %d candidate(s): %s",
                        timestamp(), len(candidates), list(candidates.keys())[:12])

            if not candidates:
                elapsed = time.time() - window_start
                to_sleep = max(1, MONITOR_DURATION - elapsed)
                logger.info("No candidates this minute — sleeping %.1fs before next full scan", to_sleep)
                time.sleep(to_sleep)
                continue

            # 2) Focused monitoring for MONITOR_DURATION seconds
            window_end = window_start + MONITOR_DURATION
            while time.time() < window_end and candidates:
                round_start = time.time()
                workers = min(MAX_WORKERS, max(4, len(candidates)))
                latest = {s: {"bin": None, "gate": None} for s in list(candidates.keys())}

                with ThreadPoolExecutor(max_workers=workers) as ex:
                    fut_map = {}
                    for sym, info in list(candidates.items()):
                        gate_sym = info["gate_sym"]
                        b_symbol = sym
                        fut_map[ex.submit(get_binance_price, b_symbol, http_session)] = ("bin", sym)
                        fut_map[ex.submit(get_gateio_price_once, gate_sym, http_session)] = ("gate", sym)

                    for fut in as_completed(fut_map):
                        typ, sym = fut_map[fut]
                        try:
                            bid, ask = fut.result()
                        except Exception:
                            bid, ask = None, None
                        if bid and ask:
                            latest[sym][typ] = {"bid": bid, "ask": ask}

                for sym in list(candidates.keys()):
                    info = candidates.get(sym)
                    if not info:
                        continue
                    b = latest[sym].get("bin")
                    g = latest[sym].get("gate")
                    if not b or not g:
                        continue
                    spread = calculate_spread(b["bid"], b["ask"], g["bid"], g["ask"])
                    if spread is None:
                        continue

                    if spread > info["max_spread"]:
                        candidates[sym]["max_spread"] = spread
                    if spread < info["min_spread"]:
                        candidates[sym]["min_spread"] = spread

                    if abs(spread) >= ALERT_THRESHOLD:
                        now = time.time()
                        cooldown_ok = (sym not in last_alert) or (now - last_alert[sym] > ALERT_COOLDOWN)
                        if not cooldown_ok:
                            logger.debug("Alert suppressed by cooldown for %s", sym)
                            candidates[sym]["alerted"] = True
                            continue

                        confirmed = False
                        for attempt in range(CONFIRM_RETRIES):
                            time.sleep(CONFIRM_RETRY_DELAY)
                            b2_bid, b2_ask = get_binance_price(sym, http_session, retries=1)
                            g2_bid, g2_ask = get_gateio_price_once(info["gate_sym"], http_session, retries=1)
                            if b2_bid and b2_ask and g2_bid and g2_ask:
                                spread2 = calculate_spread(b2_bid, b2_ask, g2_bid, g2_ask)
                                logger.debug("Confirm check %d for %s: %.4f%%", attempt+1, sym, spread2 if spread2 is not None else 0)
                                if spread2 is not None and abs(spread2) >= ALERT_THRESHOLD:
                                    confirmed = True
                                    b_confirm, g_confirm = {"bid": b2_bid, "ask": b2_ask}, {"bid": g2_bid, "ask": g2_ask}
                                    break
                        if not confirmed:
                            logger.info("False positive avoided for %s (initial %.4f%%)", sym, spread)
                            candidates[sym]["alerted"] = False
                            continue

                        direction = "Long Binance / Short Gate.io" if spread2 > 0 else "Long Gate.io / Short Binance"
                        msg = (
                            f"*BIG SPREAD ALERT*\n"
                            f"`{sym}` → *{spread2:+.4f}%*\n"
                            f"Direction → {direction}\n"
                            f"Binance: `{b_confirm['bid']:.6f}` ↔ `{b_confirm['ask']:.6f}`\n"
                            f"Gate.io: `{g_confirm['bid']:.6f}` ↔ `{g_confirm['ask']:.6f}`\n"
                            f"{timestamp()}"
                        )
                        send_telegram(msg)
                        logger.info("ALERT → %s %+.4f%% (confirmed)", sym, spread2)
                        last_alert[sym] = time.time()
                        candidates.pop(sym, None)

                elapsed = time.time() - round_start
                sleep_for = MONITOR_POLL - elapsed
                if sleep_for > 0:
                    if time.time() + sleep_for > window_end:
                        sleep_for = max(0, window_end - time.time())
                    if sleep_for > 0:
                        time.sleep(sleep_for)

            overall_max = None; overall_max_sym = None
            overall_min = None; overall_min_sym = None
            for sym, info in candidates.items():
                if overall_max is None or info["max_spread"] > overall_max:
                    overall_max, overall_max_sym = info["max_spread"], sym
                if overall_min is None or info["min_spread"] < overall_min:
                    overall_min, overall_min_sym = info["min_spread"], sym

            summary = f"*Minute Monitor Summary* — {timestamp()}\n"
            summary += f"Candidates monitored: {len(candidates)}\n"
            if overall_max_sym:
                summary += f"Max +ve → `{overall_max_sym}`: *+{overall_max:.4f}%*\n"
            else:
                summary += "No +ve spreads\n"
            if overall_min_sym:
                summary += f"Max -ve → `{overall_min_sym}`: *{overall_min:.4f}%*\n"
            else:
                summary += "No -ve spreads\n"
            send_telegram(summary)
            logger.info("Summary sent for window starting %s", timestamp())

            elapsed_total = time.time() - window_start
            if elapsed_total < MONITOR_DURATION:
                time.sleep(max(0.2, MONITOR_DURATION - elapsed_total))

            heartbeat_counter += 1
            if heartbeat_counter % 20 == 0:
                logger.info("Bot alive — %s", timestamp())

        except Exception:
            logger.exception("Fatal error in main loop, sleeping briefly before retry")
            time.sleep(5)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down.")
    except Exception:
        logger.exception("Unhandled exception at top level")
