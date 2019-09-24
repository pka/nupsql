psql plugin for Nu Shell
========================

Execute PostgreSQL commands from [Nu Shell](https://github.com/nushell/nushell).

## Usage

```
> help psql
Execute PostgreSQL query.

Usage:
  > psql <conn> <query> 
```

Usage examples:

```
> psql "host=localhost user=postgres" "SELECT * FROM pg_stat_activity" | pivot
━━━━┯━━━━━━━━━━━━━━━━━━┯━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 #  │ Column0          │ Column1 
────┼──────────────────┼────────────────────────────────
  0 │ datid            │  
  1 │ datname          │  
  2 │ pid              │                           3299 
  3 │ usesysid         │  
  4 │ usename          │  
  5 │ application_name │  
  6 │ client_addr      │  
  7 │ client_hostname  │  
  8 │ client_port      │                          36932 
  9 │ backend_start    │  
 10 │ xact_start       │  
 11 │ query_start      │  
 12 │ state_change     │  
 13 │ waiting          │  
 14 │ state            │ active 
 15 │ query            │ SELECT * FROM pg_stat_activity 
━━━━┷━━━━━━━━━━━━━━━━━━┷━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

## Installation

```
cargo build
cargo install --path .
```
