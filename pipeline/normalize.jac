"""Post normalization between adapters and the pipeline."""

import re;
import from datetime { UTC }
import from adapters.base { NormalizedPost }
import from config { Settings }

# zero-width space / non-joiner / joiner and the byte-order mark
glob _ZERO_WIDTH: re.Pattern[str] = re.compile("[​‌‍﻿]");

"""Clean and validate; returns None for posts that must not enter the pipeline."""
def normalize(post: NormalizedPost, settings: Settings) -> NormalizedPost | None {
    # adapters drop these already; defense in depth
    if post.is_reply {
        return None;
    }
    post.text = _ZERO_WIDTH.sub("", post.text or "").strip();
    if post.quoted_text {
        post.quoted_text = _ZERO_WIDTH.sub("", post.quoted_text).strip() or None;
    }
    if not post.text and not post.quoted_text {
        return None;
    }
    if post.posted_at.tzinfo is None {
        post.posted_at = post.posted_at.replace(tzinfo=UTC);
    }
    if post.received_at.tzinfo is None {
        post.received_at = post.received_at.replace(tzinfo=UTC);
    }
    # adapter default; let config override by handle
    if post.tier == "osint" {
        post.tier = settings.tier_for_handle(post.account_handle);
    }
    return post;
}
