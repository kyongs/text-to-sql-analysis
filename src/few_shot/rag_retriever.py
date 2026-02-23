# src/few_shot/rag_retriever.py
"""
Few-shot RAG Retriever
NLQ 기반으로 유사한 예제를 검색하여 few-shot으로 제공합니다.
"""

import os
import json
import numpy as np
from typing import List, Dict, Any, Tuple, Optional
from pathlib import Path
import hashlib

# OpenAI for embeddings
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()


class FewShotRetriever:
    """Few-shot 예제 검색기"""

    def __init__(self, dataset_path: str, embedding_model: str = "text-embedding-3-small"):
        """
        Args:
            dataset_path: 데이터셋 JSON 파일 경로 (question, sql 포함)
            embedding_model: OpenAI 임베딩 모델명
        """
        self.dataset_path = dataset_path
        self.embedding_model = embedding_model
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

        # 데이터 로드
        with open(dataset_path, 'r', encoding='utf-8') as f:
            self.data = json.load(f)

        # 임베딩 캐시 경로
        cache_name = hashlib.md5(dataset_path.encode()).hexdigest()[:8]
        self.cache_path = Path(dataset_path).parent / f"embeddings_cache_{cache_name}.npy"
        self.questions_cache_path = Path(dataset_path).parent / f"questions_cache_{cache_name}.json"

        # 임베딩 로드 또는 생성
        self.embeddings = None
        self.questions = None
        self._load_or_create_embeddings()

    def _load_or_create_embeddings(self):
        """임베딩 캐시 로드 또는 새로 생성"""
        if self.cache_path.exists() and self.questions_cache_path.exists():
            print(f"✅ Loading cached embeddings from {self.cache_path}")
            self.embeddings = np.load(self.cache_path)
            with open(self.questions_cache_path, 'r', encoding='utf-8') as f:
                self.questions = json.load(f)

            # 캐시된 질문 수와 현재 데이터셋 크기 비교
            if len(self.questions) != len(self.data):
                print(f"⚠️ Cache size mismatch ({len(self.questions)} vs {len(self.data)}), regenerating...")
                self._create_embeddings()
        else:
            print(f"📊 Creating embeddings for {len(self.data)} questions...")
            self._create_embeddings()

    def _create_embeddings(self):
        """모든 질문에 대해 임베딩 생성"""
        self.questions = [item['question'] for item in self.data]

        # 배치로 임베딩 생성 (API 효율성)
        batch_size = 100
        all_embeddings = []

        for i in range(0, len(self.questions), batch_size):
            batch = self.questions[i:i+batch_size]
            print(f"  Embedding batch {i//batch_size + 1}/{(len(self.questions)-1)//batch_size + 1}...")

            response = self.client.embeddings.create(
                model=self.embedding_model,
                input=batch
            )

            batch_embeddings = [e.embedding for e in response.data]
            all_embeddings.extend(batch_embeddings)

        self.embeddings = np.array(all_embeddings)

        # 캐시 저장
        np.save(self.cache_path, self.embeddings)
        with open(self.questions_cache_path, 'w', encoding='utf-8') as f:
            json.dump(self.questions, f, ensure_ascii=False)

        print(f"✅ Embeddings saved to {self.cache_path}")

    def _cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        """코사인 유사도 계산"""
        return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

    def get_similar_examples(
        self,
        query: str,
        k: int = 3,
        exclude_indices: List[int] = None
    ) -> List[Dict[str, Any]]:
        """
        쿼리와 유사한 예제 k개 검색

        Args:
            query: 검색할 질문
            k: 반환할 예제 수
            exclude_indices: 제외할 인덱스 (자기 자신 등)

        Returns:
            [{"question": str, "sql": str, "similarity": float, "index": int}, ...]
        """
        if exclude_indices is None:
            exclude_indices = []

        # 쿼리 임베딩 생성
        response = self.client.embeddings.create(
            model=self.embedding_model,
            input=[query]
        )
        query_embedding = np.array(response.data[0].embedding)

        # 모든 질문과 유사도 계산
        similarities = []
        for i, emb in enumerate(self.embeddings):
            if i in exclude_indices:
                continue
            sim = self._cosine_similarity(query_embedding, emb)
            similarities.append((i, sim))

        # 유사도 내림차순 정렬
        similarities.sort(key=lambda x: x[1], reverse=True)

        # 상위 k개 반환
        results = []
        for idx, sim in similarities[:k]:
            item = self.data[idx]
            results.append({
                "question": item['question'],
                "sql": item.get('sql', item.get('SQL', '')),
                "similarity": float(sim),
                "index": idx
            })

        return results

    def get_random_examples(
        self,
        k: int = 3,
        exclude_indices: List[int] = None,
        seed: int = None
    ) -> List[Dict[str, Any]]:
        """
        랜덤하게 예제 k개 선택

        Args:
            k: 반환할 예제 수
            exclude_indices: 제외할 인덱스
            seed: 랜덤 시드 (재현성)

        Returns:
            [{"question": str, "sql": str, "index": int}, ...]
        """
        if exclude_indices is None:
            exclude_indices = []

        # 선택 가능한 인덱스
        available_indices = [i for i in range(len(self.data)) if i not in exclude_indices]

        # 랜덤 선택
        if seed is not None:
            np.random.seed(seed)
        selected_indices = np.random.choice(available_indices, size=min(k, len(available_indices)), replace=False)

        results = []
        for idx in selected_indices:
            item = self.data[idx]
            results.append({
                "question": item['question'],
                "sql": item.get('sql', item.get('SQL', '')),
                "index": int(idx)
            })

        return results


def format_few_shot_examples(examples: List[Dict[str, Any]], include_similarity: bool = False) -> str:
    """
    Few-shot 예제를 프롬프트 형식으로 포맷

    Args:
        examples: 예제 리스트
        include_similarity: 유사도 점수 포함 여부

    Returns:
        포맷된 few-shot 예제 문자열
    """
    if not examples:
        return ""

    lines = ["### Few-shot Examples ###"]
    lines.append("Here are some similar question-SQL pairs for reference:\n")

    for i, ex in enumerate(examples, 1):
        lines.append(f"**Example {i}:**")
        if include_similarity and 'similarity' in ex:
            lines.append(f"(Similarity: {ex['similarity']:.3f})")
        lines.append(f"Question: {ex['question']}")
        lines.append(f"SQL: {ex['sql']}")
        lines.append("")

    lines.append("Now generate SQL for the given question:\n")

    return "\n".join(lines)


# CLI 테스트
if __name__ == "__main__":
    import sys

    # 테스트
    dataset_path = "data/beaver/dw/dw.json"

    if not os.path.exists(dataset_path):
        print(f"Dataset not found: {dataset_path}")
        sys.exit(1)

    retriever = FewShotRetriever(dataset_path)

    # 테스트 쿼리
    test_query = "List the unique course instructor names and the amount of material for each instructor."

    print("\n=== Semantic Search (k=3) ===")
    similar = retriever.get_similar_examples(test_query, k=3, exclude_indices=[])
    for ex in similar:
        print(f"[{ex['index']}] (sim: {ex['similarity']:.3f}) {ex['question'][:60]}...")

    print("\n=== Random Selection (k=3) ===")
    random_ex = retriever.get_random_examples(k=3, seed=42)
    for ex in random_ex:
        print(f"[{ex['index']}] {ex['question'][:60]}...")

    print("\n=== Formatted Few-shot ===")
    print(format_few_shot_examples(similar[:2], include_similarity=True))
