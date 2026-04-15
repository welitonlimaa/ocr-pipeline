def compute_text_stats(text: str):
    words = text.split()
    word_count = len(words)

    tokens_estimate = int(word_count * 0.75)

    return {
        "word_count": word_count,
        "tokens_estimate": tokens_estimate,
    }
