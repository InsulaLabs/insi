1) make custom raft commands for atomically modifying value to track bytes-stored-per-api key and enforce limits
2) Add tracking caches for each api key on each node on startup to rate limit their reads and writes per-node
3) clean interfaces between web and fsm - see about ironing out conversions/re-mappings for speed/ clarity
4) create otto js runtime wrapper to run cluster setups/ host events/ test framework for scripted integration tests