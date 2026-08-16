use std::borrow::Cow;
use std::env;

use rustyline::completion::{Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::{Hint, Hinter};
use rustyline::validate::Validator;
use rustyline::{Context, Helper};

const COMMANDS: [&str; 5] = ["help", "list", "open", "quit", "run"];

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum SessionCommand {
    Empty,
    Help,
    List,
    Open,
    Quit,
    Run(String),
    Invalid(String),
}

pub(super) fn parse(line: &str) -> SessionCommand {
    let words = line.split_whitespace().collect::<Vec<_>>();
    match words.as_slice() {
        [] => SessionCommand::Empty,
        ["help"] => SessionCommand::Help,
        ["list"] => SessionCommand::List,
        ["open"] => SessionCommand::Open,
        ["quit"] | ["exit"] => SessionCommand::Quit,
        ["run", name] => SessionCommand::Run((*name).to_owned()),
        _ => SessionCommand::Invalid(line.trim().to_owned()),
    }
}

#[derive(Clone, Debug)]
pub(super) struct PromptHelper {
    example_names: Vec<&'static str>,
    suggestion: String,
    color: bool,
}

impl PromptHelper {
    pub fn new(example_names: impl IntoIterator<Item = &'static str>) -> Self {
        Self {
            example_names: example_names.into_iter().collect(),
            suggestion: "list".to_owned(),
            color: env::var_os("NO_COLOR").is_none(),
        }
    }

    pub fn set_suggestion(&mut self, suggestion: impl Into<String>) {
        self.suggestion = suggestion.into();
    }
}

#[derive(Clone, Debug)]
pub(super) struct GhostHint(String);

impl Hint for GhostHint {
    fn display(&self) -> &str {
        &self.0
    }

    fn completion(&self) -> Option<&str> {
        Some(&self.0)
    }
}

impl Hinter for PromptHelper {
    type Hint = GhostHint;

    fn hint(&self, line: &str, pos: usize, _ctx: &Context<'_>) -> Option<Self::Hint> {
        if pos != line.len() || !self.suggestion.starts_with(line) {
            return None;
        }
        let suffix = &self.suggestion[pos..];
        (!suffix.is_empty()).then(|| GhostHint(suffix.to_owned()))
    }
}

impl Completer for PromptHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        let before_cursor = &line[..pos];
        if let Some(fragment) = before_cursor.strip_prefix("run ") {
            let candidates = self
                .example_names
                .iter()
                .filter(|name| name.starts_with(fragment))
                .map(|name| Pair {
                    display: (*name).to_owned(),
                    replacement: (*name).to_owned(),
                })
                .collect();
            return Ok((4, candidates));
        }

        let candidates = COMMANDS
            .iter()
            .filter(|command| command.starts_with(before_cursor))
            .map(|command| Pair {
                display: (*command).to_owned(),
                replacement: (*command).to_owned(),
            })
            .collect();
        Ok((0, candidates))
    }
}

impl Highlighter for PromptHelper {
    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        if self.color {
            Cow::Owned(format!("\u{1b}[2m{hint}\u{1b}[0m"))
        } else {
            Cow::Borrowed(hint)
        }
    }
}

impl Validator for PromptHelper {}
impl Helper for PromptHelper {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_keeps_the_session_command_surface_closed() {
        assert_eq!(parse(""), SessionCommand::Empty);
        assert_eq!(parse("list"), SessionCommand::List);
        assert_eq!(
            parse("run polyglot"),
            SessionCommand::Run("polyglot".to_owned())
        );
        assert_eq!(
            parse("run polyglot extra"),
            SessionCommand::Invalid("run polyglot extra".to_owned())
        );
    }

    #[test]
    fn ghost_hint_is_only_the_untyped_suffix() {
        let mut helper = PromptHelper::new(["polyglot"]);
        helper.set_suggestion("run polyglot");
        let history = rustyline::history::DefaultHistory::new();
        let context = Context::new(&history);

        assert_eq!(
            helper.hint("run p", 5, &context).unwrap().display(),
            "olyglot"
        );
        assert!(helper.hint("run p", 3, &context).is_none());
    }
}
