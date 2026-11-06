"""alelt_dst_redshift - Amazon Redshift destination (MERGE + checkpoint in one transaction)."""
from .destination import RedshiftDestination, RedshiftDialect

__all__ = ["RedshiftDestination", "RedshiftDialect"]
