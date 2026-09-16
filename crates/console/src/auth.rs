//! Who is asking, and what they are allowed to do.
//!
//! Two roles, both with powers the other does not have, because a third role
//! that can do exactly what the second can is decoration rather than access
//! control:
//!
//! * **viewer** — read the workspace: pipelines, runs, schedules, lineage.
//! * **operator** — all of that, and start a run.
//!
//! # Where a token comes from
//!
//! By default the console **mints a fresh pair at startup and prints them
//! once**, the way a local notebook server does. Nothing is stored, so there
//! is no token file to leak, no token to rotate, and a console that has been
//! stopped cannot be reached with yesterday's link.
//!
//! A stable token — for a CI job, or a console that restarts — comes from the
//! **environment**: `ETL_CONSOLE_OPERATOR_TOKEN` and `ETL_CONSOLE_VIEWER_TOKEN`.
//! Deliberately not a command-line flag: an argument is visible in the process
//! list to every other user on the machine, which is the same call this project
//! already made for `etl secret set`.
//!
//! # How a token is checked
//!
//! In **constant time**, and both tokens are always compared even once one has
//! matched. A comparison that returns early on the first wrong byte tells an
//! attacker how much of their guess was right, and one that stops at the first
//! match tells them which role they hit. Neither is expensive to avoid.
//!
//! The length of a token is not secret and is compared normally; the entropy
//! is in the bytes.
//!
//! # Where a token may appear
//!
//! In the `Authorization: Bearer` header, for everything. In a `?token=` query
//! parameter **only on the page itself**, so that the printed link works —
//! the page then holds it and sends headers from then on. A query token is not
//! accepted on the API, which is what stops a link in a chat log from being a
//! usable API credential and stops another site's form from posting one for
//! you.

use etl_secrets::random_token;

/// How many bytes of entropy a minted token carries.
///
/// 32 bytes is 256 bits, which is not guessable and is short enough to paste.
const TOKEN_BYTES: usize = 32;

/// The environment variable holding a stable operator token.
pub const OPERATOR_ENV: &str = "ETL_CONSOLE_OPERATOR_TOKEN";

/// The environment variable holding a stable viewer token.
pub const VIEWER_ENV: &str = "ETL_CONSOLE_VIEWER_TOKEN";

/// What a caller is allowed to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    /// Read the workspace.
    Viewer,
    /// Read the workspace, and start a run.
    Operator,
}

impl Role {
    pub fn name(self) -> &'static str {
        match self {
            Role::Viewer => "viewer",
            Role::Operator => "operator",
        }
    }

    /// Whether this role is enough for something needing `required`.
    ///
    /// Ordered rather than a permission set: with two roles, one of which is
    /// strictly the other plus one power, a set would be more machinery than
    /// the question deserves. A third role that is not a superset of viewer
    /// would be the moment to change this.
    pub fn allows(self, required: Role) -> bool {
        self >= required
    }
}

/// Whether a minted token was printed, and so needs showing to the person.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// Freshly minted for this process. Must be shown; it exists nowhere else.
    Minted,
    /// Taken from the environment. Must **not** be shown — it is stable, it is
    /// somebody's secret, and printing it would put it in the terminal
    /// scrollback and the CI log of every run.
    Environment,
}

/// The tokens this console accepts.
#[derive(Debug, Clone)]
pub struct Tokens {
    operator: String,
    viewer: String,
    operator_source: Source,
    viewer_source: Source,
}

impl Tokens {
    /// Take stable tokens from the environment, and mint whatever is missing.
    pub fn from_environment_or_mint() -> Self {
        Self::build(
            std::env::var(OPERATOR_ENV).ok(),
            std::env::var(VIEWER_ENV).ok(),
        )
    }

    /// The same, from explicit values — the seam the tests use.
    pub fn build(operator: Option<String>, viewer: Option<String>) -> Self {
        // An empty variable is treated as absent rather than as an empty
        // token. `ETL_CONSOLE_VIEWER_TOKEN=` in a shell script is somebody
        // clearing it, and a console that accepted "" as a password would be
        // open to anyone who sent no token at all.
        let operator = operator.filter(|token| !token.trim().is_empty());
        let viewer = viewer.filter(|token| !token.trim().is_empty());

        let operator_source = source_of(&operator);
        let viewer_source = source_of(&viewer);

        Tokens {
            operator: operator.unwrap_or_else(|| random_token(TOKEN_BYTES)),
            viewer: viewer.unwrap_or_else(|| random_token(TOKEN_BYTES)),
            operator_source,
            viewer_source,
        }
    }

    pub fn operator(&self) -> &str {
        &self.operator
    }

    pub fn viewer(&self) -> &str {
        &self.viewer
    }

    pub fn operator_source(&self) -> Source {
        self.operator_source
    }

    pub fn viewer_source(&self) -> Source {
        self.viewer_source
    }

    /// What role a presented token carries, if any.
    ///
    /// Both comparisons always run. Returning as soon as the operator token
    /// matched would make the operator check measurably faster than the viewer
    /// one, which tells an attacker which of the two they are close to.
    pub fn role_for(&self, presented: &str) -> Option<Role> {
        let is_operator = constant_time_eq(presented.as_bytes(), self.operator.as_bytes());
        let is_viewer = constant_time_eq(presented.as_bytes(), self.viewer.as_bytes());

        // Operator first, so that the degenerate case of both variables being
        // set to the same value grants the higher role rather than the lower.
        // That is the reading somebody who did it on purpose intended, and it
        // is refused outright at startup when it was not.
        if is_operator {
            Some(Role::Operator)
        } else if is_viewer {
            Some(Role::Viewer)
        } else {
            None
        }
    }

    /// Whether both roles have been given the same token.
    ///
    /// Checked at startup and refused, because it silently promotes every
    /// viewer to an operator — the one mistake in this file that looks like it
    /// is working.
    pub fn roles_collide(&self) -> bool {
        self.operator == self.viewer
    }
}

fn source_of(value: &Option<String>) -> Source {
    match value {
        Some(_) => Source::Environment,
        None => Source::Minted,
    }
}

/// Compare two byte strings without returning early on a difference.
///
/// The length is compared first and normally: it is not secret, and a loop
/// over mismatched lengths would have to invent bytes to compare. Everything
/// after that accumulates differences and only looks at the result at the end,
/// so the time taken does not depend on *where* the first difference is.
pub fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    let mut difference = 0_u8;

    for (a, b) in left.iter().zip(right.iter()) {
        difference |= a ^ b;
    }

    difference == 0
}

/// The bearer token on a request, if it has one.
///
/// Only `Authorization: Bearer <token>`. The scheme is matched
/// case-insensitively because clients differ on it, but the token itself is
/// not touched.
pub fn bearer_of(header: &str) -> Option<&str> {
    let (scheme, token) = header.trim().split_once(' ')?;

    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }

    let token = token.trim();

    if token.is_empty() {
        None
    } else {
        Some(token)
    }
}

#[cfg(test)]
mod tests;
