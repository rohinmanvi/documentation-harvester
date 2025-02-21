import re
import unicodedata
import nltk
from nltk.tokenize import word_tokenize
from nltk.util import ngrams
from collections import defaultdict
from hashlib import sha1
import resiliparse.extract.html2text as html2text

nltk.download('punkt', quiet=True)

def extract_text_from_html(html_string):
    """
    Extract plain text from HTML.
    """
    return html2text.extract_plain_text(html_string)

def gopher_quality_filter(text):
    """
    Determine whether the extracted text meets quality thresholds.
    """
    words = word_tokenize(text)
    num_words = len(words)
    if num_words < 50 or num_words > 100000:
        return False
    mean_word_length = sum(len(word) for word in words) / num_words if num_words > 0 else 0
    if mean_word_length < 3 or mean_word_length > 10:
        return False
    lines = text.split('\n')
    ellipsis_count = sum(1 for line in lines if line.endswith('...'))
    ellipsis_ratio = ellipsis_count / len(lines) if lines else 0
    if ellipsis_ratio > 0.3:
        return False
    alphabetic_words_ratio = sum(1 for word in words if any(char.isalpha() for char in word)) / num_words if num_words > 0 else 0
    if alphabetic_words_ratio < 0.7:
        return False
    return True

def normalize_text(text):
    """
    Normalize text for deduplication.
    """
    text = text.lower()
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\w\s]', '', text)
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def compute_ngrams(text, n):
    tokens = word_tokenize(text)
    return list(ngrams(tokens, n))

def minhash(ngram_list, num_hashes):
    return [min([int(sha1(f"{h}{ngram}".encode()).hexdigest(), 16) for ngram in ngram_list]) for h in range(num_hashes)]

def lsh(signatures, num_bands):
    bands = defaultdict(list)
    rows_per_band = len(signatures[0]) // num_bands
    for doc_id, sig in enumerate(signatures):
        for i in range(num_bands):
            band_signature = tuple(sig[i * rows_per_band: (i + 1) * rows_per_band])
            bands[band_signature].append(doc_id)
    return bands

def jaccard_similarity(set1, set2):
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union if union != 0 else 0

def minhash_deduplication(processed_texts, num_hashes, num_bands, ngram_length, jaccard_threshold):
    """
    Deduplicate texts using a minhash and LSH approach.
    """
    normalized_docs = [normalize_text(text) for text in processed_texts]
    ngram_sets = [set(compute_ngrams(doc, ngram_length)) for doc in normalized_docs]
    minhash_signatures = [minhash(list(ngram_set), num_hashes) for ngram_set in ngram_sets]
    
    bands = lsh(minhash_signatures, num_bands)
    
    candidate_pairs = set()
    for band in bands.values():
        if len(band) > 1:
            for i in range(len(band)):
                for j in range(i + 1, len(band)):
                    candidate_pairs.add((band[i], band[j]))
    
    to_remove = set()
    for i, j in candidate_pairs:
        if jaccard_similarity(ngram_sets[i], ngram_sets[j]) >= jaccard_threshold:
            to_remove.add(j)
    
    unique_texts = [text for idx, text in enumerate(processed_texts) if idx not in to_remove]
    return unique_texts