#!/usr/bin/env python3
"""Pump.fun Replay Sprint -- paper bot (agent-fun/replay/v2, Python, stdlib only).

Reads hello / batch / done as newline-delimited JSON on stdin and writes exactly one
reply per batch on stdout. The strategy is a launch sniper with a rug filter:

* every coin is watched from its CreateEvent; in its first slots the bot reads the
  opening tape -- the creator's own buy in the create slot (0.05-1 SOL: skin in
  the game, not a whale dump), at least 2 other wallets buying over at least 3
  trades with non-negative net flow, at most 2 outside buyers in all (a launch
  bundled by many wallets in its first slot is usually the creator's cluster
  waiting to dump), whether the creator has already sold, and how much real SOL
  the curve holds. It buys only launches that pass;
* it takes profit fast (half the position at +8%, the rest at +20% or at cost
  once the first half is banked), cuts losers at -8%, never holds past 10 slots,
  and leaves at once when the creator sells;
* order splitting keeps the run above the competition's fill pace: when the filled
  count lags its prorated target, each order is split into pieces sent in the same
  reply (the pieces fill at the same price as the whole order).

Deterministic by construction: no clock, no randomness, no environment reads; every
iteration is over insertion-ordered dicts fed by the batch stream, and the grader
fixes PYTHONHASHSEED=0. Diagnostics go to stderr; stdout carries replies only.

Run locally against the kit's dev window with:
    node cli/makefun.mjs grade-local --kit . --bot <repo> --driver process
"""

import json
import re
import sys

PROTOCOL = "agent-fun/replay/v2"
MAX_HARNESS_LINE_BYTES = 1_048_576
MAX_BOT_LINE_BYTES = 65_536
LAMPORTS = 1_000_000_000
BPS = 10_000
#: Curve fee taken on every fill (protocol 95 bps + creator 30 bps).
FEE_BPS = 125


class ProtocolError(Exception):
    """The harness sent something this bot cannot follow."""


class OrderError(Exception):
    """The bot produced something the harness would treat as a violation."""


# ---------------------------------------------------------------------------
# Strategy parameters.
# ---------------------------------------------------------------------------

PARAMS = {
    "min_age": 0,               # youngest age (slots since create) at entry
    "max_age": 3,               # oldest age at which a first buy may be decided
    "stake_lamports": 50_000_000,    # lamports to aim for per coin in total
    "min_buy_lamports": 5_000_000,   # smallest single buy worth the tx fee
    "max_adds": 0,              # buys after the first one, on later batches
    "add_step_bps": 0,          # an add needs price this far above the last buy
    "add_delay": 2,             # slots to wait after the previous buy
    "add_lamports": 100_000_000,
    "add_min_flow_lamports": 0,
    "min_real_sol": 300_000_000,      # real SOL reserve window at decision time
    "max_real_sol": 30_000_000_000,
    "min_dev_buy": 50_000_000,  # creator's own buys in the create slot
    "max_dev_buy": 1_000_000_000,
    "min_trades": 3,            # trades seen before entry
    "min_buyers": 2,            # distinct (non-creator) buyers seen before entry
    "max_buyers": 2,            # at most: a launch bundled by many wallets dumps
    "max_sell_ratio_bps": 10_000,  # lamports sold by others / bought, at most
    "max_big_sell_bps": 10_000,    # largest single non-creator sell, bps of rq
    "min_runup_bps": 0,         # price now over the create price, bps window
    "max_runup_bps": 1_000_000,
    "flow_window": 10,          # recent-flow window (slots)
    "min_flow_lamports": 0,
    "min_recent_buyers": 0,
    "skip_dev_sold": True,      # creator sold before entry: skip the coin
    "buy_slippage_bps": 10_000,
    "tp1_bps": 800,            # first take-profit (sell half at +8%)
    "tp1_part_bps": 5_000,
    "sl_after_tp1_bps": 0,      # after it, the rest stops out at cost
    "tp_bps": 2_000,            # exit when worth this much over cost (bps)
    "sl_bps": 800,              # exit when worth this much under cost (bps)
    "trail_arm_bps": 1_000_000, # trailing stop (off)
    "trail_bps": 1_000,
    "max_hold": 10,             # exit after holding this many slots
    "exit_on_dev_sell": True,
    "stale_slots": 1_000_000,   # exit when no trade for this long (off)
    "exit_flow_lamports": 1_000_000_000_000,  # exit when flow below -this (off)
    "fill_target": 300,         # guardrail pacing: fills to reach over window
    "window_seconds": 21_600,
    "base_split": 1,
    "max_split": 6,
    "min_piece_lamports": 2_000_000,
}


# ---------------------------------------------------------------------------
# Replay state: curves, positions, pending orders.
# ---------------------------------------------------------------------------

_PUBKEY_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")
_CLIENT_ID_RE = re.compile(r"[A-Za-z0-9-][A-Za-z0-9_-]{0,63}")
_U64_RE = re.compile(r"0|[1-9][0-9]{0,19}")


class Curve:
    __slots__ = ("mint", "created_slot", "vt", "vq", "rt", "rq",
                 "reserves_slot", "trades", "complete")

    def __init__(self, mint, created_slot, slot):
        self.mint = mint
        self.created_slot = created_slot
        self.vt = 0
        self.vq = 0
        self.rt = 0
        self.rq = 0
        self.reserves_slot = slot
        self.trades = 0
        self.complete = False


class State:
    def __init__(self, hello):
        self.hello = hello
        self.seq = -1
        self.slot = -1
        self.cash = int(hello["capital"]["lamports"])
        self.positions = {}   # mint -> (token_amount:int, cost_lamports:int)
        self.curves = {}      # mint -> Curve, first-seen order
        self.pending = {}     # client_id -> order dict, send order
        self._used_ids = set()
        self._next_id = 1

    def fresh_id(self):
        while True:
            cid = "o%d" % self._next_id
            self._next_id += 1
            if cid not in self._used_ids:
                self._used_ids.add(cid)
                return cid

    def tx_fee(self):
        fees = self.hello["fees"]
        return int(fees["base_fee_lamports"]) + int(fees["priority_fee_lamports"])

    def max_buy_lamports(self, mint):
        curve = self.curves.get(mint)
        if curve is None or curve.complete:
            return 0
        return curve.rq * self.hello["caps"]["max_order_bps_of_real_reserve"] // BPS

    def committed_positions(self):
        mints = set(self.positions)
        for o in self.pending.values():
            if o["side"] == "buy":
                mints.add(o["mint"])
        return len(mints)

    def free_cash(self):
        tx = self.tx_fee()
        free = self.cash
        for o in self.pending.values():
            free -= tx
            if o["side"] == "buy":
                free -= int(o["quote_lamports"])
        return free

    def apply_batch(self, batch):
        self.seq = batch["seq"]
        self.slot = batch["slot"]
        for f in batch["fills"]:
            self.pending.pop(f["client_id"], None)
        self._set_account(batch["account"])
        for m in batch["marks"]:
            curve = self.curves.get(m["mint"])
            if curve is not None and m["slot"] < curve.reserves_slot:
                continue
            if curve is None:
                curve = Curve(m["mint"], None, m["slot"])
                self.curves[m["mint"]] = curve
            curve.vt = int(m["virtual_token_reserves"])
            curve.vq = int(m["virtual_quote_reserves"])
            curve.rt = int(m["real_token_reserves"])
            curve.rq = int(m["real_quote_reserves"])
            curve.reserves_slot = m["slot"]
        for e in batch["events"]:
            kind = e["event"]
            if kind == "CreateEvent":
                curve = Curve(e["mint"], e["slot"], e["slot"])
                curve.vt = int(e["virtual_token_reserves"])
                curve.vq = int(e["virtual_quote_reserves"])
                curve.rt = int(e["real_token_reserves"])
                curve.rq = 0
                self.curves[e["mint"]] = curve
            elif kind == "TradeEvent":
                curve = self.curves.get(e["mint"])
                if curve is None:
                    curve = Curve(e["mint"], None, e["slot"])
                    self.curves[e["mint"]] = curve
                curve.vt = int(e["virtual_token_reserves"])
                curve.vq = int(e["virtual_quote_reserves"])
                curve.rt = int(e["real_token_reserves"])
                curve.rq = int(e["real_quote_reserves"])
                curve.reserves_slot = e["slot"]
                curve.trades += 1
            else:  # CompleteEvent (or any other terminal event): no more orders
                curve = self.curves.get(e["mint"])
                if curve is None:
                    curve = Curve(e["mint"], None, e["slot"])
                    self.curves[e["mint"]] = curve
                curve.complete = True

    def apply_done(self, done):
        self.seq = done["seq"]
        self._set_account(done["account"])

    def _set_account(self, account):
        self.cash = int(account["cash_lamports"])
        self.positions = {}
        for p in account["positions"]:
            tokens = int(p["token_amount"])
            if tokens > 0:
                self.positions[p["mint"]] = (tokens, int(p["cost_lamports"]))

    def check_orders(self, orders):
        cap = self.hello["caps"]["max_orders_per_batch"]
        if len(orders) > cap:
            raise OrderError("batch %d: %d orders, over max_orders_per_batch (%d)"
                             % (self.seq, len(orders), cap))
        checked = []
        taken = set()
        for i, o in enumerate(orders):
            checked.append(self._check_order(o, "batch %d order %d" % (self.seq, i)))
            if checked[-1]["client_id"] is not None:
                taken.add(checked[-1]["client_id"])
        out = []
        for o in checked:
            cid = o["client_id"] or self.fresh_id()
            taken.add(cid)
            key = "quote_lamports" if o["side"] == "buy" else "token_amount"
            out.append({"side": o["side"], "client_id": cid, "mint": o["mint"],
                        key: o["amount"], "max_slippage_bps": o["max_slippage_bps"]})
        return out

    def _check_order(self, value, where):
        if not isinstance(value, dict):
            raise OrderError("%s: expected an order dict" % where)
        side = value.get("side")
        if side not in ("buy", "sell"):
            raise OrderError('%s: side must be "buy" or "sell"' % where)
        allowed = ("side", "client_id", "mint", "quote_lamports",
                   "max_slippage_bps") if side == "buy" else (
                       "side", "client_id", "mint", "token_amount", "max_slippage_bps")
        for k in value:
            if k not in allowed:
                raise OrderError("%s: unknown key %r on a %s order" % (where, k, side))
        mint = value.get("mint")
        if not isinstance(mint, str) or not _PUBKEY_RE.fullmatch(mint):
            raise OrderError("%s: mint must be a base58 address" % where)
        key = "quote_lamports" if side == "buy" else "token_amount"
        amount = value.get(key)
        if type(amount) is int:
            n = amount
        elif isinstance(amount, str) and _U64_RE.fullmatch(amount):
            n = int(amount)
        else:
            raise OrderError("%s: %s must be a u64" % (where, key))
        if n < 0 or n > 2**64 - 1:
            raise OrderError("%s: %s out of u64 range" % (where, key))
        bps = value.get("max_slippage_bps")
        if type(bps) is not int or not 0 <= bps <= BPS:
            raise OrderError("%s: max_slippage_bps must be an int 0..10000" % where)
        cid = value.get("client_id")
        if cid is not None and (not isinstance(cid, str)
                                or not _CLIENT_ID_RE.fullmatch(cid)):
            raise OrderError("%s: bad client_id" % where)
        return {"side": side, "client_id": cid, "mint": mint,
                "amount": str(n), "max_slippage_bps": bps}

    def record_orders(self, orders):
        for o in orders:
            self._used_ids.add(o["client_id"])
            self.pending[o["client_id"]] = o


# ---------------------------------------------------------------------------
# Strategy: launch sniper with a rug filter.
# ---------------------------------------------------------------------------


def sell_value(tokens, vq, vt):
    """What selling `tokens` against (vq, vt) pays after the 1.25% curve fee."""
    if tokens <= 0 or vt <= 0:
        return 0
    q = (tokens * vq) // (vt + tokens)
    return q - (q * FEE_BPS + BPS - 1) // BPS


class Coin:
    __slots__ = ("mint", "create_slot", "creator", "vq0", "vt0", "dev_buy",
                 "trades", "buyers", "bought", "sold_by_others", "big_sell_bps",
                 "dev_sold", "last_trade_slot", "recent", "buys_sent",
                 "spent_sent", "last_buy_vq", "last_buy_vt", "last_buy_slot",
                 "done", "complete")

    def __init__(self, mint, create_slot, creator, vq0, vt0):
        self.mint = mint
        self.create_slot = create_slot
        self.creator = creator
        self.vq0 = vq0
        self.vt0 = vt0
        self.dev_buy = 0
        self.trades = 0
        self.buyers = {}       # non-creator buyer -> True, first-seen order
        self.bought = 0
        self.sold_by_others = 0
        self.big_sell_bps = 0
        self.dev_sold = False
        self.last_trade_slot = create_slot
        self.recent = []       # (slot, is_buy, sol, user), slot order
        self.buys_sent = 0
        self.spent_sent = 0
        self.last_buy_vq = 0
        self.last_buy_vt = 1
        self.last_buy_slot = 0
        self.done = False
        self.complete = False


class Held:
    __slots__ = ("opened_slot", "tx_paid", "best", "dev_sold_at_open", "took_tp1")

    def __init__(self, opened_slot, tx_paid, dev_sold_at_open):
        self.opened_slot = opened_slot
        self.tx_paid = tx_paid
        self.best = 0
        self.dev_sold_at_open = dev_sold_at_open
        self.took_tp1 = False


class Strategy:
    def __init__(self, params):
        self.p = params
        self.coins = {}    # mint -> Coin, first-seen order
        self.held = {}     # mint -> Held
        self.selling = {}  # mint -> True while a sell is in flight
        self.first_time = None
        self.now_time = 0
        self.filled = 0

    # -- helpers ------------------------------------------------------------

    def pieces(self):
        """How many pieces to split an order into now (fill pacing)."""
        p = self.p
        if self.first_time is None:
            return p["base_split"]
        elapsed = max(0, min(p["window_seconds"], self.now_time - self.first_time))
        due = p["fill_target"] * elapsed / p["window_seconds"]
        if self.filled >= due:
            return p["base_split"]
        behind = due - self.filled
        return min(p["max_split"],
                   p["base_split"] + (2 if behind > p["fill_target"] * 0.15 else 1))

    def sell_orders(self, mint, tokens, k, room):
        n = max(1, min(k, room))
        piece = tokens // n
        if piece == 0:
            return [{"side": "sell", "mint": mint, "token_amount": str(tokens),
                     "max_slippage_bps": BPS}]
        out = []
        for i in range(n):
            amt = tokens - piece * (n - 1) if i == n - 1 else piece
            out.append({"side": "sell", "mint": mint, "token_amount": str(amt),
                        "max_slippage_bps": BPS})
        return out

    def buy_orders(self, mint, amount, k, room):
        p = self.p
        n = max(1, min(k, room))
        min_piece = p["min_piece_lamports"]
        while n > 1 and amount // n < min_piece:
            n -= 1
        piece = amount // n
        out = []
        for i in range(n):
            amt = amount - piece * (n - 1) if i == n - 1 else piece
            out.append({"side": "buy", "mint": mint, "quote_lamports": str(amt),
                        "max_slippage_bps": p["buy_slippage_bps"]})
        return out

    def read_events(self, batch):
        for e in batch["events"]:
            if e["event"] == "CreateEvent":
                self.coins[e["mint"]] = Coin(
                    e["mint"], e["slot"], e["creator"],
                    int(e["virtual_quote_reserves"]),
                    int(e["virtual_token_reserves"]))
            elif e["event"] == "TradeEvent":
                c = self.coins.get(e["mint"])
                if c is None:
                    continue
                sol = int(e["sol_amount"])
                c.trades += 1
                c.last_trade_slot = e["slot"]
                c.recent.append((e["slot"], bool(e["is_buy"]), sol, e["user"]))
                is_dev = e["user"] == c.creator
                if e["is_buy"]:
                    c.bought += sol
                    if is_dev and e["slot"] == c.create_slot:
                        c.dev_buy += sol
                    elif not is_dev:
                        c.buyers[e["user"]] = True
                elif is_dev:
                    c.dev_sold = True
                else:
                    c.sold_by_others += sol
                    rq_before = int(e["real_quote_reserves"]) + sol
                    if rq_before > 0:
                        bps = sol * BPS // rq_before
                        if bps > c.big_sell_bps:
                            c.big_sell_bps = bps
            else:
                c = self.coins.get(e["mint"])
                if c is not None:
                    c.complete = True
                    c.done = True

    def flow(self, coin, slot):
        """Net lamports (bought minus sold) and distinct buyers, last flow_window."""
        p = self.p
        start = slot - p["flow_window"]
        drop = 0
        while drop < len(coin.recent) and coin.recent[drop][0] <= start:
            drop += 1
        if drop:
            del coin.recent[:drop]
        net = 0
        buyers = {}
        for (_slot, is_buy, sol, user) in coin.recent:
            if is_buy:
                net += sol
                if user != coin.creator:
                    buyers[user] = True
            else:
                net -= sol
        return net, len(buyers)

    def has_pending_buy(self, state, mint):
        return any(o["mint"] == mint and o["side"] == "buy"
                   for o in state.pending.values())

    def has_pending_sell(self, state, mint):
        return any(o["mint"] == mint and o["side"] == "sell"
                   for o in state.pending.values())

    # -- main callback --------------------------------------------------------

    def on_batch(self, state, batch):
        p = self.p
        self.read_events(batch)
        for e in batch["events"]:
            bt = e.get("block_time")
            if bt is None:
                continue
            if self.first_time is None:
                self.first_time = bt
            if bt > self.now_time:
                self.now_time = bt
        orders = []
        cap = state.hello["caps"]["max_orders_per_batch"]
        tx = state.tx_fee()
        marks = {m["mint"]: (int(m["virtual_quote_reserves"]),
                             int(m["virtual_token_reserves"]))
                 for m in batch["marks"]}

        for f in batch["fills"]:
            if f["client_id"].startswith("_"):
                self.held.pop(f["mint"], None)
                self.selling.pop(f["mint"], None)
                continue
            if f.get("status") == "filled":
                self.filled += 1
            h = self.held.get(f["mint"])
            if f.get("status") != "rejected" and h is not None:
                h.tx_paid += int(f["tx_fee_lamports"])
            if (f.get("side") == "buy" and f.get("status") == "filled"
                    and h is None):
                c = self.coins.get(f["mint"])
                self.held[f["mint"]] = Held(
                    f["slot"], int(f["tx_fee_lamports"]),
                    c.dev_sold if c is not None else False)
        for mint in list(self.selling):
            if not self.has_pending_sell(state, mint):
                self.selling.pop(mint)
        for mint in list(self.held):
            if (mint not in state.positions and mint not in self.selling
                    and not self.has_pending_buy(state, mint)):
                self.held.pop(mint)
        k = self.pieces()

        # -- exits first ----------------------------------------------------
        for mint, pos in list(state.positions.items()):
            if len(orders) >= cap:
                break
            if mint in self.selling:
                continue
            c = self.coins.get(mint)
            h = self.held.get(mint)
            if h is None:
                h = Held(state.slot, tx, c.dev_sold if c is not None else False)
                self.held[mint] = h
            mk = marks.get(mint)
            if mk is None:
                curve = state.curves.get(mint)
                if curve is None:
                    continue
                mk = (curve.vq, curve.vt)
            tokens, cost_lamports = pos
            value = sell_value(tokens, mk[0], mk[1]) - tx
            basis = cost_lamports + h.tx_paid
            if value > h.best:
                h.best = value
            exit_now = False
            if basis > 0:
                if (not h.took_tp1
                        and value * BPS >= basis * (BPS + p["tp1_bps"])):
                    part = tokens * p["tp1_part_bps"] // BPS
                    if 0 < part < tokens:
                        orders.append({"side": "sell", "mint": mint,
                                       "token_amount": str(part),
                                       "max_slippage_bps": BPS})
                        self.selling[mint] = True
                        h.took_tp1 = True
                        h.best = 0
                        if c is not None:
                            c.done = True
                        continue
                if value * BPS >= basis * (BPS + p["tp_bps"]):
                    exit_now = True
                sl = p["sl_after_tp1_bps"] if h.took_tp1 else p["sl_bps"]
                if value * BPS <= basis * (BPS - sl):
                    exit_now = True
                if (h.best * BPS >= basis * (BPS + p["trail_arm_bps"])
                        and value * BPS <= h.best * (BPS - p["trail_bps"])):
                    exit_now = True
            if state.slot - h.opened_slot >= p["max_hold"]:
                exit_now = True
            if c is not None:
                if p["exit_on_dev_sell"] and c.dev_sold and not h.dev_sold_at_open:
                    exit_now = True
                if state.slot - c.last_trade_slot >= p["stale_slots"]:
                    exit_now = True
                if self.flow(c, state.slot)[0] < -p["exit_flow_lamports"]:
                    exit_now = True
            if not exit_now:
                continue
            orders.extend(self.sell_orders(mint, tokens, k, cap - len(orders)))
            self.selling[mint] = True
            if c is not None:
                c.done = True

        # -- entries and adds ----------------------------------------------
        free = state.free_cash() - tx * len(orders)
        open_count = state.committed_positions()
        for c in self.coins.values():
            if len(orders) >= cap:
                break
            if c.done or c.complete:
                continue
            age = state.slot - c.create_slot
            is_add = c.buys_sent > 0
            if not is_add and age > p["max_age"]:
                c.done = True
                continue
            if age < p["min_age"]:
                continue
            if is_add:
                if c.buys_sent > p["max_adds"]:
                    continue
                if (c.mint not in state.positions or c.mint in self.selling
                        or self.has_pending_buy(state, c.mint)):
                    continue
            elif open_count >= state.hello["caps"]["max_open_positions"]:
                continue
            curve = state.curves.get(c.mint)
            if curve is None or curve.complete:
                continue
            if c.dev_sold and (p["skip_dev_sold"] or is_add):
                if not is_add:
                    c.done = True
                continue
            vq, vt = curve.vq, curve.vt
            if is_add:
                # A second tranche once the launch showed it is no instant dump.
                if state.slot - c.last_buy_slot < p["add_delay"]:
                    continue
                if vq * c.last_buy_vt * BPS < c.last_buy_vq * vt * (BPS + p["add_step_bps"]):
                    continue
                if self.flow(c, state.slot)[0] < p["add_min_flow_lamports"]:
                    continue
                max_add = state.max_buy_lamports(c.mint)
                amount = min(p["add_lamports"], max_add)
                if amount < p["min_buy_lamports"] or free < amount + tx * 6:
                    continue
                add = self.buy_orders(c.mint, amount, k, cap - len(orders))
                orders.extend(add)
                free -= amount + tx * len(add)
                c.buys_sent += 1
                c.spent_sent += amount
                c.last_buy_vq, c.last_buy_vt = vq, vt
                c.last_buy_slot = state.slot
                continue
            # Rug filter.
            rq = curve.rq
            if rq < p["min_real_sol"] or rq > p["max_real_sol"]:
                continue
            if c.dev_buy < p["min_dev_buy"] or c.dev_buy > p["max_dev_buy"]:
                continue
            if (c.trades < p["min_trades"] or len(c.buyers) < p["min_buyers"]
                    or len(c.buyers) > p["max_buyers"]):
                continue
            if (c.bought > 0
                    and c.sold_by_others * BPS > c.bought * p["max_sell_ratio_bps"]):
                continue
            if c.big_sell_bps > p["max_big_sell_bps"]:
                continue
            runup = (vq * c.vt0 * BPS) // (vt * c.vq0) if vt and c.vq0 else 0
            if runup > p["max_runup_bps"] or runup < p["min_runup_bps"]:
                continue
            fl_net, fl_buyers = self.flow(c, state.slot)
            if fl_net < p["min_flow_lamports"] or fl_buyers < p["min_recent_buyers"]:
                continue
            room = p["stake_lamports"] - c.spent_sent
            if room < p["min_buy_lamports"]:
                continue
            max_buy = state.max_buy_lamports(c.mint)
            amount = min(room, max_buy)
            if amount < p["min_buy_lamports"]:
                continue
            if free < amount + tx * 6:
                continue
            buys = self.buy_orders(c.mint, amount, k, cap - len(orders))
            orders.extend(buys)
            free -= amount + tx * len(buys)
            c.buys_sent += 1
            c.spent_sent += amount
            c.last_buy_vq, c.last_buy_vt = vq, vt
            c.last_buy_slot = state.slot
            open_count += 1
        return orders


# ---------------------------------------------------------------------------
# Wire loop
# ---------------------------------------------------------------------------


def read_lines(src):
    line_no = 0
    while True:
        raw = src.readline(MAX_HARNESS_LINE_BYTES + 1)
        if not raw:
            return
        line_no += 1
        if raw.endswith(b"\n"):
            raw = raw[:-1]
        elif len(raw) > MAX_HARNESS_LINE_BYTES:
            raise ProtocolError("input line %d is over %d bytes"
                                % (line_no, MAX_HARNESS_LINE_BYTES))
        if raw.endswith(b"\r"):
            raw = raw[:-1]
        try:
            yield raw.decode("utf-8")
        except UnicodeDecodeError:
            raise ProtocolError("input line %d is not valid UTF-8" % line_no) from None


def run(strategy):
    state = None
    batches = 0
    out = sys.stdout
    for line in read_lines(sys.stdin.buffer):
        if not line.strip():
            continue
        try:
            msg = json.loads(line)
        except ValueError:
            raise ProtocolError("input is not JSON") from None
        kind = msg.get("type") if isinstance(msg, dict) else None
        if kind == "hello":
            if state is not None:
                raise ProtocolError("a second hello")
            if msg.get("protocol") != PROTOCOL:
                raise ProtocolError("the harness speaks %r; this bot speaks %s"
                                    % (msg.get("protocol"), PROTOCOL))
            state = State(msg)
        elif kind == "batch":
            if state is None:
                raise ProtocolError("a batch before hello")
            if not (type(msg.get("seq")) is int and type(msg.get("slot")) is int
                    and isinstance(msg.get("events"), list)
                    and isinstance(msg.get("marks"), list)
                    and isinstance(msg.get("fills"), list)
                    and isinstance(msg.get("account"), dict)):
                raise ProtocolError("malformed batch")
            state.apply_batch(msg)
            orders = state.check_orders(strategy.on_batch(state, msg))
            text = json.dumps({"type": "reply", "seq": msg["seq"], "orders": orders},
                              separators=(",", ":"), ensure_ascii=False)
            if len(text.encode("utf-8")) > MAX_BOT_LINE_BYTES:
                raise OrderError("batch %d: reply over %d bytes"
                                 % (msg["seq"], MAX_BOT_LINE_BYTES))
            state.record_orders(orders)
            out.write(text + "\n")
            out.flush()
            batches += 1
        elif kind == "done":
            if state is None:
                raise ProtocolError("done before hello")
            state.apply_done(msg)
            print("done: %d batches, final cash %d lamports"
                  % (batches, state.cash), file=sys.stderr)
            return
        else:
            raise ProtocolError("unknown message type %r" % (kind,))
    if state is None:
        raise ProtocolError("input ended before hello")
    raise ProtocolError("input ended before done (after %d batches)" % batches)


if __name__ == "__main__":
    run(Strategy(PARAMS))
