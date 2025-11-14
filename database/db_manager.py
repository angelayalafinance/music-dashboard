# database/db_manager.py
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from .db import SessionLocal

class DatabaseManager:
    """Simplified database interface for CRUD operations"""
    
    def __init__(self):
        self.session_factory = SessionLocal
    
    def bulk_insert(self, data: List[Dict], model: Any) -> bool:
        """Bulk insert records"""
        session = self.session_factory()
        try:
            session.bulk_insert_mappings(model, data)
            session.commit()
            return True
        except SQLAlchemyError as e:
            session.rollback()
            return False
        finally:
            session.close()
    
    def get_all(self, model: Any, filters: Dict = None) -> List[Any]:
        """Get all records with optional filtering"""
        session = self.session_factory()
        try:
            query = session.query(model)
            if filters:
                for key, value in filters.items():
                    if hasattr(model, key):
                        query = query.filter(getattr(model, key) == value)
            return query.all()
        finally:
            session.close()
    
    def get_one(self, model: Any, filters: Dict) -> Optional[Any]:
        """Get a single record matching filters"""
        session = self.session_factory()
        try:
            query = session.query(model)
            for key, value in filters.items():
                if hasattr(model, key):
                    query = query.filter(getattr(model, key) == value)
            return query.first()
        finally:
            session.close()

    def delete(self, model: Any, filters: Dict) -> bool:
        """Delete records matching filters"""
        session = self.session_factory()
        try:
            query = session.query(model)
            for key, value in filters.items():
                if hasattr(model, key):
                    query = query.filter(getattr(model, key) == value)
            query.delete()
            session.commit()
            return True
        except SQLAlchemyError as e:
            session.rollback()
            return False
        finally:
            session.close()

    def update(self, model: Any, filters: Dict, updates: Dict) -> bool:
        """Update records matching filters with new values"""
        session = self.session_factory()
        try:
            query = session.query(model)
            for key, value in filters.items():
                if hasattr(model, key):
                    query = query.filter(getattr(model, key) == value)
            query.update(updates)
            session.commit()
            return True
        except SQLAlchemyError as e:
            session.rollback()
            return False
        finally:
            session.close()