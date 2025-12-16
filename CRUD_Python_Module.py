# CRUD_Python_Module.py
from typing import Any, Dict, List, Optional
from pymongo import MongoClient
from pymongo.errors import PyMongoError


class AnimalShelter(object):
    """CRUD operations for Animal collection in MongoDB"""

    def __init__(
        self,
        USER: str = "aacuser",
        PASS: str = "ILoveAnimals92!",
        HOST: str = "localhost",
        PORT: int = 27017,
        DB: str = "aac",
        COL: str = "animals",
        auth_source: str = "admin",
        tls: bool = False,
    ):
        """
        Initialize the MongoClient
        """
        uri = (
            f"mongodb://{USER}:{PASS}@{HOST}:{PORT}/"
            f"?authSource={auth_source}&tls={'true' if tls else 'false'}"
        )
        self.client = MongoClient(
            host=HOST,
            port=PORT,
            username=USER,
            password=PASS,
            authSource=auth_source,
            tls=tls
        )
        self.database = self.client[DB]
        self.collection = self.database[COL]
        
    # helper: return next available integer for a field
    def get_next_record_number(self, field: str = "record_no") -> int:
        """
        Finds the current max(field) and returns max+1.
        If field doesn't exist yet, returns 1.
        """
        try:
            doc = (
                self.collection.find({field: {"$type": "int"}})
                .sort(field, -1)
                .limit(1)
                .next()
            )
            return int(doc.get(field, 0)) + 1
        except StopIteration:
            return 1
        except Exception:
            # If anything odd happens, fall back safely
            return 1

    # ---- C in CRUD ----
    def create(self, data: Dict[str, Any]) -> bool:
        """
        Insert a single document.
        Returns True if acknowledged, else False.
        """
        if not isinstance(data, dict) or not data:
            raise Exception("Nothing to save, data parameter is empty or not a dict")

        try:
            result = self.collection.insert_one(data)
            return bool(result.acknowledged and result.inserted_id)
        except PyMongoError as e:
            print(f"[CREATE ERROR] {e}")
            return False

    # ---- R in CRUD ----
    def read(
        self,
        query: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, int]] = None,
        limit: int = 0,
        sort: Optional[List[tuple]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Query for documents using find() (not find_one()) and return a list.
        """
        try:
            cursor = self.collection.find(query or {}, projection or None)
            if sort:
                cursor = cursor.sort(sort)
            if isinstance(limit, int) and limit > 0:
                cursor = cursor.limit(limit)
            return list(cursor)
        except PyMongoError as e:
            print(f"[READ ERROR] {e}")
            return []
    # ---------------- U ----------------
    def update(self,
               query: Dict[str, Any],
               new_values: Dict[str, Any],
               many: bool = True) -> int:
        """
        Update one or many documents matching `query` using $set on `new_values`.
        Returns the number of documents modified.
        """
        if not isinstance(query, dict) or not isinstance(new_values, dict):
            raise Exception("Query and new_values must be dicts")
        try:
            update_doc = {"$set": new_values}
            if many:
                result = self.collection.update_many(query, update_doc)
            else:
                result = self.collection.update_one(query, update_doc)
            return int(result.modified_count)
        except PyMongoError as e:
            print(f"[UPDATE ERROR] {e}")
            return 0

    # ---------------- D ----------------
    def delete(self,
               query: Dict[str, Any],
               many: bool = True) -> int:
        """
        Delete one or many documents matching `query`.
        Returns the number of documents removed.
        """
        if not isinstance(query, dict):
            raise Exception("Query must be a dict")
        try:
            if many:
                result = self.collection.delete_many(query)
            else:
                result = self.collection.delete_one(query)
            return int(result.deleted_count)
        except PyMongoError as e:
            print(f"[DELETE ERROR] {e}")
            return 0