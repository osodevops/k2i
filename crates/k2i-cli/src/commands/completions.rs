//! Completion and man-page generation.

use crate::{Cli, CompletionAction};
use anyhow::{Context, Result};
use clap::{Arg, Command, CommandFactory};
use clap_complete::{generate, Shell};
use std::path::Path;

/// Generate shell completions or recursive man pages.
pub(crate) fn run(action: CompletionAction) -> Result<()> {
    match action {
        CompletionAction::Bash => generate_shell_completion(Shell::Bash),
        CompletionAction::Zsh => generate_shell_completion(Shell::Zsh),
        CompletionAction::Fish => generate_shell_completion(Shell::Fish),
        CompletionAction::PowerShell => generate_shell_completion(Shell::PowerShell),
        CompletionAction::Man { output_dir } => {
            std::fs::create_dir_all(&output_dir).with_context(|| {
                format!(
                    "failed to create man page output directory {}",
                    output_dir.display()
                )
            })?;
            let cmd = Cli::command();
            render_man_tree(&cmd, &output_dir, "k2i")?;
            eprintln!("Man pages written to {}", output_dir.display());
            Ok(())
        }
    }
}

fn generate_shell_completion(shell: Shell) -> Result<()> {
    let mut cmd = Cli::command();
    generate(shell, &mut cmd, "k2i", &mut std::io::stdout());
    Ok(())
}

fn render_man_tree(cmd: &Command, out_dir: &Path, page_name: &str) -> Result<()> {
    let page_cmd = cmd.clone().name(page_name.to_string());
    let man = clap_mangen::Man::new(page_cmd);
    let mut buf = Vec::new();
    man.render(&mut buf)
        .with_context(|| format!("failed to render man page for {}", page_name))?;
    std::fs::write(out_dir.join(format!("{page_name}.1")), buf)
        .with_context(|| format!("failed to write man page for {}", page_name))?;

    if cmd.has_subcommands() {
        render_help_page(out_dir, page_name)?;
    }

    for subcmd in cmd.get_subcommands() {
        if subcmd.get_name() == "help" {
            continue;
        }
        let child_page_name = format!("{page_name}-{}", subcmd.get_name());
        render_man_tree(subcmd, out_dir, &child_page_name)?;
    }

    Ok(())
}

fn render_help_page(out_dir: &Path, parent_page_name: &str) -> Result<()> {
    let page_name = format!("{parent_page_name}-help");
    let help_cmd = Command::new(page_name.clone())
        .about("Print this message or the help of the given subcommand(s)")
        .arg(
            Arg::new("subcommands")
                .value_name("subcommands")
                .num_args(0..)
                .help("Command path to print help for"),
        );
    let man = clap_mangen::Man::new(help_cmd);
    let mut buf = Vec::new();
    man.render(&mut buf)
        .with_context(|| format!("failed to render man page for {}", page_name))?;
    std::fs::write(out_dir.join(format!("{page_name}.1")), buf)
        .with_context(|| format!("failed to write man page for {}", page_name))
}
