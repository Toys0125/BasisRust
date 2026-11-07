use crossterm::{
    cursor, execute,
    terminal::{self, EnterAlternateScreen, LeaveAlternateScreen},
};
use std::{
    io::{self, Stdout},
    sync::{Mutex, PoisonError},
};
use tokio::sync::watch;

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

/// Coordinates the live terminal view with process shutdown without joining
/// the console thread, which may be blocked in Rustyline's stdin read.
pub struct LiveViewCoordinator {
    closed: Mutex<bool>,
    active: watch::Sender<bool>,
}

impl LiveViewCoordinator {
    pub fn new() -> Self {
        let (active, _) = watch::channel(false);
        Self {
            closed: Mutex::new(false),
            active,
        }
    }

    /// Runs terminal initialization while holding the shutdown gate. Shutdown
    /// therefore either sees this view as active or prevents its initialization.
    pub fn begin_view(
        &self,
        initialize: impl FnOnce() -> Box<dyn FnOnce()>,
    ) -> Option<LiveViewGuard<'_>> {
        let closed = self.closed.lock().unwrap_or_else(PoisonError::into_inner);
        if *closed {
            return None;
        }

        self.active.send_replace(true);
        let mut view = LiveViewGuard {
            cleanup: None,
            _active: ActiveViewMarker { coordinator: self },
        };
        view.cleanup = Some(initialize());
        drop(closed);
        Some(view)
    }

    /// Closes the entry gate. Any view already initializing has completed that
    /// initialization before this method returns.
    pub fn begin_shutdown(&self) {
        *self.closed.lock().unwrap_or_else(PoisonError::into_inner) = true;
    }

    /// Waits only for an active live view to restore its terminal state.
    pub async fn wait_for_cleanup(&self) {
        let mut active = self.active.subscribe();
        while *active.borrow() {
            if active.changed().await.is_err() {
                return;
            }
        }
    }
}

pub struct LiveViewGuard<'a> {
    // Field order matters: drop the terminal guard closure before acknowledging
    // inactivity, so a successful wait means restoration has been attempted.
    cleanup: Option<Box<dyn FnOnce()>>,
    _active: ActiveViewMarker<'a>,
}

impl Drop for LiveViewGuard<'_> {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
        }
    }
}

struct ActiveViewMarker<'a> {
    coordinator: &'a LiveViewCoordinator,
}

impl Drop for ActiveViewMarker<'_> {
    fn drop(&mut self) {
        self.coordinator.active.send_replace(false);
    }
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
    use std::{
        panic::{catch_unwind, AssertUnwindSafe},
        sync::{
            atomic::{AtomicBool, Ordering},
            mpsc, Arc,
        },
        thread,
        time::Duration,
    };

    #[tokio::test]
    async fn shutdown_waits_for_live_view_restoration_before_acknowledging() {
        let coordinator = Arc::new(LiveViewCoordinator::new());
        let restored = Arc::new(AtomicBool::new(false));
        let active_view = coordinator
            .begin_view({
                let restored = Arc::clone(&restored);
                move || {
                    Box::new(move || {
                        restored.store(true, Ordering::SeqCst);
                    })
                }
            })
            .unwrap();

        coordinator.begin_shutdown();
        let wait = tokio::spawn({
            let coordinator = Arc::clone(&coordinator);
            async move { coordinator.wait_for_cleanup().await }
        });
        tokio::task::yield_now().await;
        assert!(
            !wait.is_finished(),
            "active live view must hold the barrier"
        );
        assert!(coordinator
            .begin_view(|| panic!("shutdown must prevent terminal initialization"))
            .is_none());

        drop(active_view);
        tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("live-view cleanup acknowledgment timed out")
            .unwrap();
        assert!(restored.load(Ordering::SeqCst));
    }

    #[test]
    fn shutdown_does_not_wait_for_a_console_thread_blocked_in_input() {
        let coordinator = LiveViewCoordinator::new();
        let (release, blocked_input) = mpsc::channel::<()>();
        let input_thread = thread::spawn(move || blocked_input.recv().unwrap());

        coordinator.begin_shutdown();
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async {
                tokio::time::timeout(Duration::from_millis(100), coordinator.wait_for_cleanup())
                    .await
            })
            .expect("inactive terminal view should not wait for Rustyline input");
        assert!(!input_thread.is_finished());

        release.send(()).unwrap();
        input_thread.join().unwrap();
    }

    #[test]
    fn partial_live_view_initialization_unwinds_before_cleanup_ack() {
        let coordinator = LiveViewCoordinator::new();
        let restored = Arc::new(AtomicBool::new(false));
        let initialized = Arc::new(AtomicBool::new(false));
        let restored_on_drop = Arc::clone(&restored);

        let result = catch_unwind(AssertUnwindSafe(|| {
            let _ = coordinator.begin_view(|| {
                struct PartialMode(Arc<AtomicBool>);
                impl Drop for PartialMode {
                    fn drop(&mut self) {
                        self.0.store(true, Ordering::SeqCst);
                    }
                }
                initialized.store(true, Ordering::SeqCst);
                let _partial_mode = PartialMode(restored_on_drop);
                panic!("simulated panic during terminal setup");
            });
        }));

        assert!(result.is_err());
        assert!(initialized.load(Ordering::SeqCst));
        assert!(restored.load(Ordering::SeqCst));
        coordinator.begin_shutdown();
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(coordinator.wait_for_cleanup());
    }

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
