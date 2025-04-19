"""
Cassandra database connection utility.
"""
import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table

class CassandraClient:
    """Singleton class for managing Cassandra connection."""
    _instance = None
    _session = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CassandraClient, cls).__new__(cls)
            cls._instance._connect()
        return cls._instance
    
    def _connect(self):
        """Establish connection to Cassandra."""
        CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
        CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
        CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")
        
        cluster = Cluster([CASSANDRA_HOST])
        self._session = cluster.connect(CASSANDRA_KEYSPACE)
    
    @property
    def session(self):
        """Get the Cassandra session."""
        return self._session
    
    def execute(self, query, params=None):
        """Execute a CQL query."""
        return self.session.execute(query, params or {})
    
    def execute_async(self, query, params=None):
        """Execute a CQL query asynchronously."""
        return self.session.execute_async(query, params or {}) 