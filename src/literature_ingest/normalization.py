import unicodedata

def normalize_document(data: str) -> str:
    # Step 1: Normalise with NFKC
    normalised_data = unicodedata.normalize("NFKC", data)

    # Step 2: Normalise hyphens
    norm_mapping = {
        ord("\u2010"): "-",  # HYPHEN (‐) to HYPHEN-MINUS (-)
        ord(
            "\u2011"
        ): "-",  # NON-BREAKING HYPHEN (‑) to HYPHEN-MINUS (-)
        ord("\u2013"): "-",  # EN DASH (–) to HYPHEN-MINUS (-)
        ord("\u2014"): "-",  # EM DASH (—) to HYPHEN-MINUS (-)
        ord(
            "\u2212"
        ): "-",  # MINUS SIGN (−) to HYPHEN-MINUS (-)
        ord(
            "\u1806"
        ): "-",  # SOFT HYPHEN (᠆) to HYPHEN-MINUS (-)
    }
    normalised_data = normalised_data.translate(norm_mapping)
    return normalised_data
