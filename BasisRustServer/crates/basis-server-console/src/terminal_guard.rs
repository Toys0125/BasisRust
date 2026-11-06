use crossterm::{
    cursor, execute,
    terminal::{self, EnterAlternateScreen, LeaveAlternateScreen},
};
use std::io::{self, Stdout};

#[derive(Default)]
struct TerminalModes {
    raw: bool,
    cursor_hidden: bool,
    alternate_screen: bool,
}

/// Restores terminal modes whenever the live status view returns or unwinds.
pub struct TerminalGuard {
    modes: TerminalModes,
    stdout: Stdout,
}

impl TerminalGuard {
    pub fn new() -> Self {
        Self {
            modes: TerminalModes::default(),
            stdout: io::stdout(),
        }
    }

    pub fn enable_raw_mode(&mut self) -> io::Result<()> {
        terminal::enable_raw_mode()?;
        self.modes.raw = true;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn hide_cursor(&mut self) -> io::Result<()> {
        execute!(self.stdout, cursor::Hide)?;
        self.modes.cursor_hidden = true;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn enter_alternate_screen(&mut self) -> io::Result<()> {
        execute!(self.stdout, EnterAlternateScreen)?;
        self.modes.alternate_screen = true;
        Ok(())
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let modes = &mut self.modes;
        let stdout = &mut self.stdout;
        restore_modes(modes, |mode| match mode {
            RestoreMode::ShowCursor => execute!(stdout, cursor::Show),
            RestoreMode::LeaveAlternateScreen => execute!(stdout, LeaveAlternateScreen),
            RestoreMode::DisableRawMode => terminal::disable_raw_mode(),
        });
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RestoreMode {
    ShowCursor,
    LeaveAlternateScreen,
    DisableRawMode,
}

fn restore_modes(
    modes: &mut TerminalModes,
    mut restore: impl FnMut(RestoreMode) -> io::Result<()>,
) {
    for (active, action) in [
        (&mut modes.cursor_hidden, RestoreMode::ShowCursor),
        (
            &mut modes.alternate_screen,
            RestoreMode::LeaveAlternateScreen,
        ),
        (&mut modes.raw, RestoreMode::DisableRawMode),
    ] {
        if *active {
            *active = false;
            let _ = restore(action);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restores_every_active_mode_even_if_a_restore_step_fails() {
        let mut modes = TerminalModes {
            raw: true,
            cursor_hidden: true,
            alternate_screen: true,
        };
        let mut attempted = Vec::new();

        restore_modes(&mut modes, |mode| {
            attempted.push(mode);
            if mode == RestoreMode::ShowCursor {
                Err(io::Error::other("simulated terminal write failure"))
            } else {
                Ok(())
            }
        });

        assert_eq!(
            attempted,
            [
                RestoreMode::ShowCursor,
                RestoreMode::LeaveAlternateScreen,
                RestoreMode::DisableRawMode
            ]
        );
        assert!(!modes.raw && !modes.cursor_hidden && !modes.alternate_screen);
    }

    #[test]
    fn restores_only_modes_that_were_entered() {
        let mut modes = TerminalModes {
            raw: true,
            ..TerminalModes::default()
        };
        let mut attempted = Vec::new();

        restore_modes(&mut modes, |mode| {
            attempted.push(mode);
            Ok(())
        });

        assert_eq!(attempted, [RestoreMode::DisableRawMode]);
    }
}
