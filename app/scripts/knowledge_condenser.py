import re
import networkx as nx
from typing import List
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
from sklearn.cluster import KMeans

from app.utils.simple_sentence_split import simple_sentence_split
from app.utils.stopwords import STOPWORDS_PT


class KnowledgeCondenser:
    def __init__(
        self,
        n_topics: int = 10,
        sentences_per_topic: int = 8,
        language: str = "portuguese",
    ):
        self.n_topics = n_topics
        self.sentences_per_topic = sentences_per_topic
        self.language = language

    def split_sentences(self, texts: List[str]) -> List[str]:
        sentences = []
        for text in texts:
            clean = re.sub(r"\s+", " ", text)
            sentences.extend(simple_sentence_split(clean))
        return [s.strip() for s in sentences if len(s) > 20]

    def compute_tfidf(self, sentences: List[str]):
        vectorizer = TfidfVectorizer(
            stop_words=STOPWORDS_PT,
            ngram_range=(1, 2),
            max_df=0.85,
            min_df=2,
            sublinear_tf=True,
        )

        tfidf_matrix = vectorizer.fit_transform(sentences)
        return tfidf_matrix, vectorizer

    def apply_lsa(self, tfidf_matrix):
        svd = TruncatedSVD(n_components=self.n_topics)
        lsa_matrix = svd.fit_transform(tfidf_matrix)
        return lsa_matrix

    def cluster_sentences(self, lsa_matrix):
        kmeans = KMeans(n_clusters=self.n_topics, random_state=42, n_init=10)
        labels = kmeans.fit_predict(lsa_matrix)
        return labels

    def hybrid_rank(self, sentences: List[str], tfidf_matrix):
        similarity_matrix = (tfidf_matrix * tfidf_matrix.T).toarray()

        graph = nx.from_numpy_array(similarity_matrix)
        textrank_scores = nx.pagerank(graph)

        tfidf_scores = tfidf_matrix.sum(axis=1).A1

        final_scores = []
        for i, s in enumerate(sentences):
            score = 0.7 * textrank_scores[i] + 0.3 * tfidf_scores[i]
            final_scores.append((score, s))

        ranked = sorted(final_scores, reverse=True)

        return [s for _, s in ranked]

    def condense(self, texts: List[str]) -> str:
        sentences = self.split_sentences(texts)

        if len(sentences) < 10:
            return "\n".join(sentences)

        tfidf_matrix, _ = self.compute_tfidf(sentences)
        lsa_matrix = self.apply_lsa(tfidf_matrix)
        labels = self.cluster_sentences(lsa_matrix)

        cluster_results = []

        for topic in range(self.n_topics):
            idx = [i for i, l in enumerate(labels) if l == topic]

            if not idx:
                continue

            cluster_sentences = [sentences[i] for i in idx]
            cluster_tfidf = tfidf_matrix[idx]

            ranked = self.hybrid_rank(cluster_sentences, cluster_tfidf)

            selected = ranked[: self.sentences_per_topic]

            cluster_results.append({"topic": topic, "sentences": selected})

        final_sentences = []

        max_len = max(len(c["sentences"]) for c in cluster_results)

        for i in range(max_len):
            for c in cluster_results:
                if i < len(c["sentences"]):
                    final_sentences.append(c["sentences"][i])

        seen = set()
        unique = []
        for s in final_sentences:
            if s not in seen:
                unique.append(s)
                seen.add(s)

        return "\n\n".join(unique)
