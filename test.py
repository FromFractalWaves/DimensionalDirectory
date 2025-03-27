# prototype.py
import numpy as np
import torch
from transformers import AutoModel, AutoTokenizer
from scipy.spatial.distance import cosine
from typing import Dict

class HologramConstructor:
    def __init__(self, model_configs: Dict[str, str]):
        self.models = {}
        for name, pretrained in model_configs.items():
            self.models[name] = (
                AutoModel.from_pretrained(pretrained),
                AutoTokenizer.from_pretrained(pretrained)
            )

    def construct(self, text: str, dedup_threshold: float = 0.95) -> Dict:
        # Generate embeddings from each model
        embeddings = {}
        for name, (model, tokenizer) in self.models.items():
            inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True)
            with torch.no_grad():
                outputs = model(**inputs)
            # Use [CLS] token embedding (first token)
            embeddings[name] = outputs.last_hidden_state[0, 0].numpy()

        # Deduplicate embeddings (prune near-identical ones)
        unique_embs = {}
        for name, emb in embeddings.items():
            if not any(1 - cosine(emb, kept) > dedup_threshold for kept in unique_embs.values()):
                unique_embs[name] = emb

        # Compute connections (similarity between unique embeddings)
        connections = {}
        model_names = list(unique_embs.keys())
        for i, m1 in enumerate(model_names):
            for m2 in model_names[i+1:]:
                sim = 1 - cosine(unique_embs[m1], unique_embs[m2])
                connections[f"{m1}-{m2}"] = sim

        return {"embeddings": unique_embs, "connections": connections}

class Synthesizer:
    def __init__(self, strategy: str = "weighted_average"):
        self.strategy = strategy

    def synthesize(self, hologram: Dict) -> np.ndarray:
        embeddings = hologram["embeddings"]
        if len(embeddings) == 0:
            raise ValueError("No embeddings to synthesize")

        if self.strategy == "weighted_average" and len(embeddings) > 1:
            connections = hologram["connections"]
            total_weight = sum(connections.values()) or 1
            weights = {pair: w / total_weight for pair, w in connections.items()}
            model_weights = {m: 0 for m in embeddings}
            for pair, w in weights.items():
                m1, m2 = pair.split("-")
                model_weights[m1] += w / 2
                model_weights[m2] += w / 2
            synthetic = np.zeros_like(next(iter(embeddings.values())))
            for model, emb in embeddings.items():
                synthetic += model_weights[model] * emb
            return synthetic
        else:  # Fallback to mean if no connections or single embedding
            return np.mean(list(embeddings.values()), axis=0)

# Quick test
if __name__ == "__main__":
    # Define two models
    models = {
        "bert": "bert-base-uncased",
        "distilbert": "distilbert-base-uncased"
    }

    # Initialize modules
    constructor = HologramConstructor(models)
    synthesizer = Synthesizer(strategy="weighted_average")

    # Input text
    text = "Hello world"

    # Build hologram
    hologram = constructor.construct(text, dedup_threshold=0.95)
    print("Hologram:")
    print(f"Embeddings: {list(hologram['embeddings'].keys())}")
    print(f"Connections: {hologram['connections']}")

    # Synthesize embedding
    synthetic_emb = synthesizer.synthesize(hologram)
    print(f"\nSynthetic Embedding Shape: {synthetic_emb.shape}")
    print(f"Sample Values: {synthetic_emb[:5]}")

    # Quick similarity check
    bert_emb = hologram["embeddings"].get("bert", np.zeros_like(synthetic_emb))
    distil_emb = hologram["embeddings"].get("distilbert", np.zeros_like(synthetic_emb))
    print(f"Similarity to BERT: {1 - cosine(synthetic_emb, bert_emb):.4f}")
    print(f"Similarity to DistilBERT: {1 - cosine(synthetic_emb, distil_emb):.4f}")