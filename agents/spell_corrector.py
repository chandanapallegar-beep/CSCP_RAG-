from spellchecker import SpellChecker
import re

spell = SpellChecker()

DOMAIN_WORDS = [
    # business terms
    "llc", "LLC", "inc", "corp", "corporation", "ltd", "company",
    # chemical terms
    "cas", "cdph", "cscp", "titanium", "dioxide", "acetaldehyde",
    "formaldehyde", "cetyl", "parabens", "triclosan", "phthalates",
    "silica", "talc", "mica", "retinol", "niacinamide", "hyaluronic",
    "glycerin", "petrolatum", "lanolin", "bismuth", "chromium",
    "carmine", "cocamide", "diethanolamine", "butylated", "hydroxyanisole",
    # company names
    "revlon", "avon", "loreal", "maybelline", "colgate", "palmolive",
    "strivectin", "oasis", "cosmopharm", "ventura", "waxie",
    # action words
    "summarize", "summarise", "discontinued", "reformulated",
    # query terms
    "cas", "cdphid", "chemicals", "cosmetics", "reported",
]

spell.word_frequency.load_words(DOMAIN_WORDS)


def correct_spelling(text: str) -> dict:
    original     = text
    corrected_text = text
    corrections  = {}

    words = re.findall(r"[a-zA-Z]+", text)

    for word in words:
        lower = word.lower()

        # skip short words, numbers, domain words, uppercase abbreviations
        if len(lower) <= 2:
            continue
        if lower in DOMAIN_WORDS:
            continue
        if word.isupper():          # skip ALL CAPS words like LLC, CAS
            continue
        if word[0].isupper() and len(word) > 1 and word[1:].islower() is False:
            continue                # skip mixed case like "StriVectin"

        if lower not in spell:
            suggestion = spell.correction(lower)
            if suggestion and suggestion != lower:
                corrections[word] = suggestion
                corrected_text = re.sub(
                    rf"\b{re.escape(word)}\b",
                    suggestion,
                    corrected_text,
                    flags=re.IGNORECASE
                )

    return {
        "original":      original,
        "corrected":     corrected_text,
        "corrections":   corrections,
        "was_corrected": len(corrections) > 0
    }