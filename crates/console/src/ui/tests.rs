//! The page. Mostly one property, and it is the one that matters.

use super::*;

#[test]
fn the_page_carries_the_workspace_label() {
    let page = page("my-workspace");

    assert!(
        page.contains("my-workspace"),
        "the label is not on the page"
    );
    assert!(page.contains("etl console"));
}

#[test]
fn a_label_with_markup_in_it_is_escaped() {
    // The one place server-side data meets markup in this file.
    let page = page(r#"<script>alert(1)</script>"#);

    assert!(
        !page.contains("<script>alert(1)</script>"),
        "the label was not escaped"
    );
    assert!(
        page.contains("&lt;script&gt;"),
        "the label was not escaped as text"
    );
}

#[test]
fn a_label_with_a_quote_cannot_break_out_of_an_attribute() {
    let page = page(r#"a" onload="evil()"#);

    assert!(
        !page.contains(r#"onload="evil()"#),
        "a quote escaped its attribute"
    );
    assert!(page.contains("&quot;"));
}

#[test]
fn escape_covers_the_five_characters_that_matter() {
    assert_eq!(escape("&"), "&amp;");
    assert_eq!(escape("<"), "&lt;");
    assert_eq!(escape(">"), "&gt;");
    assert_eq!(escape("\""), "&quot;");
    assert_eq!(escape("'"), "&#39;");
    // And leaves everything else alone, including non-ASCII.
    assert_eq!(escape("plain — text"), "plain — text");
}

#[test]
fn the_page_never_turns_a_string_into_markup() {
    // The rule the module docs state. A pipeline named `<img onerror=...>`
    // becomes script the moment this stops being true, and it would run with
    // the operator's token in hand.
    let page = page("workspace");

    for forbidden in [
        "innerHTML",
        "outerHTML",
        "insertAdjacentHTML",
        "document.write",
    ] {
        assert!(
            !page.contains(forbidden),
            "the page used {forbidden}, which makes workspace data into markup"
        );
    }
}

#[test]
fn the_page_takes_the_token_out_of_the_url() {
    // Otherwise it sits in the address bar to be shoulder-read or pasted.
    let page = page("workspace");

    assert!(
        page.contains("replaceState"),
        "the token is not removed from the URL"
    );
    assert!(page.contains(r#"searchParams.delete("token")"#));
}

#[test]
fn the_page_sends_the_token_as_a_header() {
    let page = page("workspace");

    assert!(
        page.contains("Authorization"),
        "the token is not sent as a header"
    );
    assert!(page.contains("Bearer "));
}

#[test]
fn the_page_loads_nothing_from_anywhere_else() {
    // The content security policy forbids it; this checks the page does not
    // try, so the two cannot drift into a console that is broken in a browser
    // and passing in a test.
    let page = page("workspace");

    assert!(
        !page.contains("https://") && !page.contains("http://"),
        "the page references an external origin"
    );
    assert!(!page.contains("<script src"), "the page loads a script");
    assert!(!page.contains("<link"), "the page loads a stylesheet");
}

#[test]
fn the_page_keeps_the_token_out_of_anything_that_outlives_the_tab() {
    // Session storage dies with the tab. Local storage would leave an
    // operator's token on the machine after the console had been stopped.
    let page = page("workspace");

    assert!(page.contains("sessionStorage"));
    assert!(
        !page.contains("localStorage"),
        "the token would outlive the console"
    );
}
