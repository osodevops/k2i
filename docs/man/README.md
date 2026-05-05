# K2I Man Pages

Generated roff man pages live in `man1/`.

Regenerate after changing CLI help text, flags, subcommands, or version:

```bash
cargo run -p k2i-cli -- completions man --output-dir docs/man/man1
```

Install locally:

```bash
sudo cp docs/man/man1/k2i*.1 /usr/local/share/man/man1/
mandb 2>/dev/null || true
```

Read directly from the repository:

```bash
man ./docs/man/man1/k2i.1
man ./docs/man/man1/k2i-table-register.1
```
