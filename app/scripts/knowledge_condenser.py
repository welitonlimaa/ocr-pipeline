import re
import time
import networkx as nx
from typing import List
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
from sklearn.cluster import KMeans

from app.utils.simple_sentence_split import simple_sentence_split
from app.utils.stopwords import STOPWORDS_PT
from app.config.logging_config import get_logger

logger = get_logger(__name__)


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
        result = [s.strip() for s in sentences if len(s) > 20]
        logger.debug(
            "Sentencas extraídas para condensação",
            extra={
                "action": "sentences_split",
                "input_texts": len(texts),
                "sentences_extracted": len(result),
            },
        )
        return result

    def compute_tfidf(self, sentences: List[str]):
        try:
            vectorizer = TfidfVectorizer(
                stop_words=STOPWORDS_PT,
                ngram_range=(1, 2),
                max_df=0.85,
                min_df=2,
                sublinear_tf=True,
            )
            tfidf_matrix = vectorizer.fit_transform(sentences)
            logger.debug(
                "TF-IDF computado",
                extra={
                    "action": "tfidf_computed",
                    "sentences": len(sentences),
                    "vocab_size": len(vectorizer.vocabulary_),
                    "matrix_shape": list(tfidf_matrix.shape),
                },
            )
            return tfidf_matrix, vectorizer
        except Exception as exc:
            logger.error(
                "Falha ao computar TF-IDF",
                extra={
                    "action": "tfidf_failed",
                    "sentences_count": len(sentences),
                    "error": str(exc),
                },
                exc_info=True,
            )
            raise

    def apply_lsa(self, tfidf_matrix):
        try:
            n_components = min(self.n_topics, tfidf_matrix.shape[1])
            if n_components != self.n_topics:
                logger.warning(
                    "Número de componentes LSA ajustado por vocabulário insuficiente",
                    extra={
                        "action": "lsa_components_adjusted",
                        "requested": self.n_topics,
                        "adjusted_to": n_components,
                        "vocab_size": tfidf_matrix.shape[1],
                    },
                )
            svd = TruncatedSVD(n_components=n_components)
            lsa_matrix = svd.fit_transform(tfidf_matrix)
            return lsa_matrix
        except Exception as exc:
            logger.error(
                "Falha ao aplicar LSA (SVD)",
                extra={"action": "lsa_failed", "error": str(exc)},
                exc_info=True,
            )
            raise

    def cluster_sentences(self, lsa_matrix):
        try:
            n_clusters = min(self.n_topics, lsa_matrix.shape[0])
            if n_clusters != self.n_topics:
                logger.warning(
                    "Número de clusters KMeans ajustado por amostras insuficientes",
                    extra={
                        "action": "kmeans_clusters_adjusted",
                        "requested": self.n_topics,
                        "adjusted_to": n_clusters,
                    },
                )
            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            labels = kmeans.fit_predict(lsa_matrix)
            return labels
        except Exception as exc:
            logger.error(
                "Falha no clustering KMeans",
                extra={"action": "kmeans_failed", "error": str(exc)},
                exc_info=True,
            )
            raise

    def hybrid_rank(self, sentences: List[str], tfidf_matrix):
        try:
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
        except Exception as exc:
            logger.error(
                "Falha no ranking híbrido TextRank+TF-IDF",
                extra={
                    "action": "hybrid_rank_failed",
                    "sentences_count": len(sentences),
                    "error": str(exc),
                },
                exc_info=True,
            )
            raise

    def condense(self, texts: List[str]) -> str:
        t0 = time.time()
        ctx = {
            "n_topics": self.n_topics,
            "sentences_per_topic": self.sentences_per_topic,
        }

        sentences = self.split_sentences(texts)

        if len(sentences) < 10:
            logger.warning(
                "Texto insuficiente para condensação completa — retornando concatenação simples",
                extra={
                    **ctx,
                    "action": "condense_fallback_short",
                    "sentences_found": len(sentences),
                },
            )
            return "\n".join(sentences)

        tfidf_matrix, _ = self.compute_tfidf(sentences)
        lsa_matrix = self.apply_lsa(tfidf_matrix)
        labels = self.cluster_sentences(lsa_matrix)

        cluster_results = []
        empty_clusters = 0

        for topic in range(self.n_topics):
            idx = [i for i, l in enumerate(labels) if l == topic]

            if not idx:
                empty_clusters += 1
                continue

            cluster_sentences = [sentences[i] for i in idx]
            cluster_tfidf = tfidf_matrix[idx]

            ranked = self.hybrid_rank(cluster_sentences, cluster_tfidf)
            selected = ranked[: self.sentences_per_topic]
            cluster_results.append({"topic": topic, "sentences": selected})

        if empty_clusters:
            logger.debug(
                "Clusters vazios ignorados na condensação",
                extra={
                    **ctx,
                    "action": "empty_clusters",
                    "empty_count": empty_clusters,
                },
            )

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

        duplicates_removed = len(final_sentences) - len(unique)
        elapsed = round(time.time() - t0, 2)

        logger.info(
            "Condensação concluída",
            extra={
                **ctx,
                "action": "condense_complete",
                "input_sentences": len(sentences),
                "output_sentences": len(unique),
                "duplicates_removed": duplicates_removed,
                "clusters_used": len(cluster_results),
                "elapsed_seconds": elapsed,
            },
        )

        return "\n\n".join(unique)
