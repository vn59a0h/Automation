def prepare_cluster_name(dlSchemaName, banner_list, dlTableName):
    cluster_names = []
    for b in banner_list:
        # Convert to lowercase and replace underscores with hyphens
        schema = dlSchemaName.lower().replace('_', '-')
        banner = b.strip().lower()
        table = dlTableName.lower().replace('_', '-')
        cluster_name = f"{schema}-{banner}-{table}"
        cluster_names.append(cluster_name)
    return cluster_names

def prepare_table_name(banner_list, dlTableName):
    table_names = []
    for b in banner_list:
        # Convert banner and table name to lowercase with underscores
        banner = b.strip().lower()
        table = dlTableName.lower()
        table_name = f"{banner}_{table}"
        table_names.append(table_name)
    return table_names