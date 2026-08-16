use std::env;
use std::fmt::{self, Display, Formatter};
use std::io::{self, IsTerminal};

#[derive(Clone, Copy, Debug)]
pub enum Tone {
    Accent,
    Error,
    Muted,
    Strong,
    Success,
    Warning,
}

impl Tone {
    fn ansi(self) -> &'static str {
        match self {
            Self::Accent => "36",
            Self::Error => "31",
            Self::Muted => "2",
            Self::Strong => "1",
            Self::Success => "32",
            Self::Warning => "33",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TerminalStyle {
    enabled: bool,
}

impl TerminalStyle {
    pub fn stdout() -> Self {
        Self::new(io::stdout().is_terminal())
    }

    pub fn stderr() -> Self {
        Self::new(io::stderr().is_terminal())
    }

    fn new(is_terminal: bool) -> Self {
        Self {
            enabled: is_terminal && env::var_os("NO_COLOR").is_none(),
        }
    }

    pub fn paint<'a>(self, tone: Tone, value: &'a str) -> Painted<'a> {
        Painted {
            enabled: self.enabled,
            tone,
            value,
        }
    }
}

pub struct Painted<'a> {
    enabled: bool,
    tone: Tone,
    value: &'a str,
}

impl Display for Painted<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        if self.enabled {
            write!(formatter, "\x1b[{}m{}\x1b[0m", self.tone.ansi(), self.value)
        } else {
            formatter.write_str(self.value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_style_is_plain_text() {
        assert_eq!(
            TerminalStyle { enabled: false }
                .paint(Tone::Success, "Ready")
                .to_string(),
            "Ready"
        );
    }

    #[test]
    fn enabled_style_wraps_only_the_value() {
        assert_eq!(
            TerminalStyle { enabled: true }
                .paint(Tone::Error, "Failed")
                .to_string(),
            "\x1b[31mFailed\x1b[0m"
        );
    }
}
