def generate_dag_name(banner, tableLoadType, dlSchemaName, dlTableName):
    """Generate formatted script string from input parameters."""
    if isinstance(banner, list):
        results = []
        for b in banner:
            results.append(f"INTLDLDAT-SA{b}-{tableLoadType}-{dlSchemaName.upper()}-{b}_{dlTableName}")
        return results
    return f"INTLDLDAT-SA{banner}-{tableLoadType}-{dlSchemaName.upper()}-{banner}_{dlTableName}"

