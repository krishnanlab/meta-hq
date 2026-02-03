"""
Helper info for validate command.
"""

from pathlib import Path
import hashlib

def get_files_to_check(doi):
    if doi == "18462463":
        FILES_TO_CHECK = [
            [Path("annotations") / "combined__level-sample.bson", "07c761c25eeb37787b13e3f0be7ea4db"],
            [Path("annotations") / "combined__level-series.bson", "4a70f6c1a4cdd693cf33b50c561e960b"],
            [Path("metadata") / "metadata__level-sample.parquet", "2527ffbc7b7bacd6054841b395975b8b"],
            [Path("metadata") / "metadata__level-series.parquet", "0215ea97cf1e09e98a43d219b53b254f"],
            [Path("metadata") / "technologies.parquet", "a7cd45dc7db09d30fe35676d8d449b32"],
            [Path("ontology") / "ontology_search.duckdb", "5108270059472abc4a18e0ee3a7c68f2"],
            [Path("ontology") / "mondo" / "id_map.parquet", "37230bd391ec3af4be4f710ab2d76707"],
            [Path("ontology") / "mondo" / "names_synonyms.json", "24318fa528ded695943b53787c0c7b00"],
            [Path("ontology") / "mondo" / "relations.parquet", "9d460ad0eaa06d85767717e2f6456163"],
            [Path("ontology") / "mondo" / "systems.txt", "dbf3e5566b4dd80e458b2cd5813ad693"],
            [Path("ontology") / "uberon_ext" / "id_map.parquet", "a449bed0812bacfa35d9b83628da594e"],
            [Path("ontology") / "uberon_ext" / "names_synonyms.json", "9e9fd448715929351e8c0e4b9561d56c"],
            [Path("ontology") / "uberon_ext" / "relations.parquet", "a9c3cead75ac43c2be3b961d69d25d53"],
            [Path("ontology") / "uberon_ext" / "systems.txt", "8e2b4b0943ae52720463257d67ec8fbf"],  
        ]
    if doi == "17666183":
        FILES_TO_CHECK = [
            [Path("annotations") / "combined__level-sample.bson", "5d41627b6194b34e19bda2edf9289b6d"],
            [Path("annotations") / "combined__level-series.bson", "c4280f1432a001ebe031d6eb4d7b5c5c"],
            [Path("metadata") / "metadata__level-sample.parquet", "5ab5771210d31f6cc3d81c29fec20a7e"],
            [Path("metadata") / "metadata__level-series.parquet", "06629fd37a62905e79e31e8eaddf021d"],
            [Path("metadata") / "technologies.parquet", "a7cd45dc7db09d30fe35676d8d449b32"],
            [Path("ontology") / "ontology_search.duckdb", "5108270059472abc4a18e0ee3a7c68f2"],
            [Path("ontology") / "mondo" / "id_map.parquet", "37230bd391ec3af4be4f710ab2d76707"],
            [Path("ontology") / "mondo" / "names_synonyms.json", "24318fa528ded695943b53787c0c7b00"],
            [Path("ontology") / "mondo" / "relations.parquet", "9d460ad0eaa06d85767717e2f6456163"],
            [Path("ontology") / "mondo" / "systems.txt", "dbf3e5566b4dd80e458b2cd5813ad693"],
            [Path("ontology") / "uberon_ext" / "id_map.parquet", "a449bed0812bacfa35d9b83628da594e"],
            [Path("ontology") / "uberon_ext" / "names_synonyms.json", "9e9fd448715929351e8c0e4b9561d56c"],
            [Path("ontology") / "uberon_ext" / "relations.parquet", "a9c3cead75ac43c2be3b961d69d25d53"],
            [Path("ontology") / "uberon_ext" / "systems.txt", "b5327f3e6591768bcdb24b1b083cd0ea"],  
        ]
    return FILES_TO_CHECK

def md5_file(filepath):
    """Calculate MD5 checksum of a file"""
    hash_md5 = hashlib.md5()
    
    with open(filepath, 'rb') as f:
        # Read in chunks to handle large files efficiently
        for chunk in iter(lambda: f.read(4096), b''):
            hash_md5.update(chunk)
    
    return hash_md5.hexdigest()
    
def check_md5_match(config_doi, config_data_dir):
    """Check MD5 checksum match"""
    FILES_TO_CHECK =  get_files_to_check(config_doi)
    changed_files = []
    for afile in FILES_TO_CHECK:
        filepath = Path(config_data_dir) / afile[0]
        new_checksum =  md5_file(filepath)
        old_checksum = afile[1]
        if new_checksum != old_checksum:
            changed_files.append(filepath)
    return changed_files