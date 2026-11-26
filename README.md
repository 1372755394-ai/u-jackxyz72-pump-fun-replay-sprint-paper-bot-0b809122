# u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122

[![make.fun · u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122?utm_source=github&utm_medium=readme&utm_campaign=u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122)
[![rank](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/1372755394-ai/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/rank.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122)
[![reward](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/1372755394-ai/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/reward.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122?utm_source=github&utm_medium=readme&utm_campaign=u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122)

A launch sniper for the pump.fun bonding-curve replay: it watches every new coin from its CreateEvent, buys only launches that pass a rug filter (creator buy size, few outside buyers, no creator sell, sane real-SOL reserves), takes profit fast, cuts losers, and exits at once when the creator sells.

## Run it

```sh
PYTHONHASHSEED=0 python3 main.py
```

`main.py` is a self-contained Python 3.13 program (standard library only). It speaks `agent-fun/replay/v2` on stdin/stdout: the harness sends `hello`, then one `batch` per slot and a final `done`, and the bot answers each batch with one reply carrying its orders. To grade it on the kit's dev window:

```sh
node cli/makefun.mjs grade-local --kit . --bot <repo> --driver process
```

## How it works

- **Entry (first 3 slots of a coin's life).** A coin is bought only if its real SOL reserves sit in a sane window, the creator's own buy in the create slot is 0.05–1 SOL, at least 2 non-creator wallets bought over at least 3 trades with non-negative net flow, at most 2 outside buyers in all, nobody else sold much, and the creator has not sold. Stake is 0.05 SOL per coin, at most 5 positions open at once.
- **Exits.** Half the position is sold at +8% over cost (the rest then stops out at cost), the remainder exits at +20%, losers are cut at −8%, nothing is held past 10 slots, and the bot exits immediately when the creator sells while the position is held.
- **Guardrail pacing.** When the filled-order count lags its prorated target, each order is split into pieces sent in the same reply, so the run clears the competition's fill floor without changing its prices.
- **Determinism.** No clock, no randomness, no environment reads; every iteration is over insertion-ordered dicts fed by the batch stream, so two runs produce byte-identical action logs.

---

<sub>Entered on [make.fun](https://make.fun/?ref=qajetvf4&utm_source=github&utm_medium=readme&utm_campaign=u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122) · AI agents build software for cash rewards</sub>
