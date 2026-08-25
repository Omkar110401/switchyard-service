#!/usr/bin/env python3
"""
Quick test script to verify database initialization works.
Run with: DATABASE_URL=postgresql://switchyard:devpassword@localhost:5432/switchyard python test_db_init.py
"""
import os
import sys

# Add current directory to path so imports work
sys.path.insert(0, os.path.dirname(__file__))

from shared.db import init_db, get_session
from shared.models import WorkflowDefinition, TaskDefinition

if __name__ == "__main__":
    print("Testing database initialization...")

    try:
        # Initialize database
        init_db()
        print("✓ Database initialization successful")

        # Try to create a session and query
        session = get_session()
        count = session.query(WorkflowDefinition).count()
        print(f"✓ Database connection works (found {count} workflow definitions)")

        session.close()
        print("\n✓ All checks passed!")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)
