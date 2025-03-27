# app/core/dd_manager.py
import uuid
import os
import sqlite3
import h5py
import numpy as np
from typing import Dict, List, Tuple, Optional
from transformers import AutoModel, AutoTokenizer
import torch
from app.core.content_mapper import DocumentMapper
from app.core.lstable_manager import LStableManager
from app.services.processors.text import TextDocumentProcessor

class DimensionalDirectory:
    def __init__(self, base_path: str, dbidL: str = "DocumentData", batch_type: str = "sentence"):
        self.base_path = base_path
        self.dbidL = dbidL
        self.directory = os.path.join(base_path, dbidL)
        self.hdf5_path = os.path.join(self.directory, "AB.hdf5")
        self.db_path = os.path.join(self.directory, "metadata.db")
        self.processor = TextDocumentProcessor(batch_type=batch_type)
        self.model = AutoModel.from_pretrained("bert-base-uncased")
        self.tokenizer = AutoTokenizer.from_pretrained("bert-base-uncased")
        self.lstable_manager = LStableManager(self.db_path, self.base_path)
        self.document_mapper = DocumentMapper(self, self.db_path, self.hdf5_path)
        os.makedirs(self.directory, exist_ok=True)
        self._init_storage()

    def _init_storage(self):
        with h5py.File(self.hdf5_path, "a") as f:
            if "inputs" not in f:
                f.create_group("inputs")
            f.attrs["dbidL"] = self.dbidL

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS inputs (
                    uuid TEXT PRIMARY KEY,
                    original_text TEXT,
                    unit_count INTEGER,
                    batch_type TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS units (
                    uuid TEXT PRIMARY KEY,
                    text TEXT,
                    hash TEXT UNIQUE,
                    embedding BLOB
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS mappings (
                    input_uuid TEXT,
                    unit_addr TEXT,
                    unit_uuid TEXT,
                    token_addr TEXT,
                    token_text TEXT,
                    embeddingID TEXT,
                    FOREIGN KEY (input_uuid) REFERENCES inputs(uuid),
                    FOREIGN KEY (unit_uuid) REFERENCES units(uuid)
                )
            """)
            conn.commit()

    def _generate_embedding(self, text: str) -> np.ndarray:
        inputs = self.tokenizer(text, return_tensors="pt", padding=True, truncation=True)
        with torch.no_grad():
            outputs = self.model(**inputs)
        return outputs.last_hidden_state[0, 0].numpy()

    def process_input(self, raw_input: str) -> Dict[str, str]:
        input_uuid = str(uuid.uuid4())
        units = self.processor.preprocess(raw_input)  # [(addr, text, hash)]
        unit_count = len(units)

        if not units:
            raise ValueError("No valid units found in input text")

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO inputs (uuid, original_text, unit_count, batch_type) VALUES (?, ?, ?, ?)",
                (input_uuid, raw_input, unit_count, self.processor.batch_type)
            )

            for unit_addr, unit_text, unit_hash in units:
                full_unit_addr = f"{input_uuid}-{unit_addr}"
                unit_uuid = self._get_or_create_unit(unit_text, unit_hash, conn)

                tokens = self.processor.tokenize(unit_text)
                for token_addr, token_text in tokens:
                    full_token_addr = f"{input_uuid}-{unit_addr}-{token_addr}"
                    embeddingID = f"token-{full_token_addr}"
                    conn.execute(
                        "INSERT INTO mappings (input_uuid, unit_addr, unit_uuid, token_addr, token_text, embeddingID) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (input_uuid, full_unit_addr, unit_uuid, token_addr, token_text, embeddingID)
                    )
            conn.commit()

        with h5py.File(self.hdf5_path, "a") as f:
            input_group = f["inputs"].create_group(input_uuid)
            input_group.attrs["unit_count"] = unit_count
            for unit_addr, unit_text, _ in units:
                full_unit_addr = f"{input_uuid}-{unit_addr}"
                unit_group = input_group.create_group(full_unit_addr)
                unit_group.attrs["original_unit"] = unit_text
                tokens = self.processor.tokenize(unit_text)
                token_addrs, token_texts = zip(*tokens) if tokens else ([], [])
                embeddings = np.array([self._generate_embedding(t) for t in token_texts]) if token_texts else np.array([])
                unit_group.create_dataset("tokens", data=token_texts if token_texts else np.array([]))
                unit_group.create_dataset("token_addrs", data=token_addrs if token_addrs else np.array([]))
                unit_group.create_dataset("embeddings", data=embeddings if embeddings.size else np.array([]))

        return {"uuid": input_uuid, "unit_count": str(unit_count)}

    def _get_or_create_unit(self, text: str, unit_hash: str, conn) -> str:
        cursor = conn.execute("SELECT uuid FROM units WHERE hash = ?", (unit_hash,))
        result = cursor.fetchone()
        if result:
            return result[0]

        unit_uuid = str(uuid.uuid4())
        embedding = self._generate_embedding(text)
        conn.execute(
            "INSERT INTO units (uuid, text, hash, embedding) VALUES (?, ?, ?, ?)",
            (unit_uuid, text, unit_hash, embedding.tobytes())
        )
        return unit_uuid

    def get_token_data(self, input_uuid: str, unit_addr: str = None) -> Dict:
        token_data = {}
        with h5py.File(self.hdf5_path, "r") as f:
            if "inputs" not in f or input_uuid not in f["inputs"]:
                raise ValueError(f"Input {input_uuid} not found")
            input_group = f["inputs"][input_uuid]
            with sqlite3.connect(self.db_path) as conn:
                if unit_addr:
                    full_unit_addr = f"{input_uuid}-{unit_addr}"
                    if full_unit_addr not in input_group:
                        raise ValueError(f"Unit {unit_addr} not found")
                    unit_group = input_group[full_unit_addr]
                    cursor = conn.execute(
                        "SELECT unit_uuid FROM mappings WHERE input_uuid = ? AND unit_addr = ? LIMIT 1",
                        (input_uuid, full_unit_addr)
                    )
                    unit_uuid = cursor.fetchone()[0]
                    token_data[full_unit_addr] = {
                        "unit_uuid": unit_uuid,
                        "original_unit": unit_group.attrs["original_unit"]
                    }
                else:
                    for full_unit_addr in input_group:
                        unit_group = input_group[full_unit_addr]
                        cursor = conn.execute(
                            "SELECT unit_uuid FROM mappings WHERE input_uuid = ? AND unit_addr = ? LIMIT 1",
                            (input_uuid, full_unit_addr)
                        )
                        unit_uuid = cursor.fetchone()[0]
                        token_data[full_unit_addr] = {
                            "unit_uuid": unit_uuid,
                            "original_unit": unit_group.attrs["original_unit"]
                        }
        return token_data

    def list_inputs(self) -> List[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT uuid, unit_count, batch_type FROM inputs")
            return [{"uuid": row[0], "unit_count": row[1], "batch_type": row[2]} for row in cursor.fetchall()]

    def tokenize_input(self, raw_input: str) -> Dict[str, List[Dict]]:
        input_uuid = str(uuid.uuid4())
        units = self.processor.preprocess(raw_input)
        unit_count = len(units)

        if not units:
            raise ValueError("No valid units found in input text")

        tokens_list = []
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO inputs (uuid, original_text, unit_count, batch_type) VALUES (?, ?, ?, ?)",
                (input_uuid, raw_input, unit_count, self.processor.batch_type)
            )
            for unit_addr, unit_text, unit_hash in units:
                full_unit_addr = f"{input_uuid}-{unit_addr}"
                unit_uuid = self._get_or_create_unit(unit_text, unit_hash, conn)
                tokens = self.processor.tokenize(unit_text)
                for token_addr, token_text in tokens:
                    full_token_addr = f"{input_uuid}-{unit_addr}-{token_addr}"
                    embeddingID = f"token-{full_token_addr}"
                    conn.execute(
                        "INSERT INTO mappings (input_uuid, unit_addr, unit_uuid, token_addr, token_text, embeddingID) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (input_uuid, full_unit_addr, unit_uuid, token_addr, token_text, embeddingID)
                    )
                    tokens_list.append({
                        "unit_addr": full_unit_addr,
                        "token_addr": token_addr,
                        "token_text": token_text,
                        "unit_uuid": unit_uuid
                    })
            conn.commit()
        return {"uuid": input_uuid, "tokens": tokens_list}