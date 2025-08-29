from pymongo import MongoClient
import pandas as pd
from collections.abc import MutableMapping


def flatten_dict(d, parent_key='', sep='.'):
    """Recursively flattens a nested dictionary"""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Leave lists as is for now, handle later
            items.append((new_key, v))
        else:
            items.append((new_key, v))
    return dict(items)


def flatten_documents(docs):
    """Fully flatten a list of MongoDB documents into tabular format with array expansion"""
    # Convert BSON to strings for unsupported types
    for d in docs:
        for k, v in d.items():
            if not isinstance(v, (str, int, float, bool, type(None), list, dict)):
                d[k] = str(v)

    # Flatten dicts
    flat_docs = [flatten_dict(d) for d in docs]
    df = pd.DataFrame(flat_docs)

    # Expand arrays into separate rows
    array_cols = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, list)).any()]

    for col in array_cols:
        df = df.explode(col, ignore_index=True)
        # If exploded element is dict, flatten it again
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            expanded = pd.json_normalize(df[col]).add_prefix(f"{col}.")
            df = df.drop(columns=[col]).reset_index(drop=True).join(expanded)

    return df


def run_queries_and_export_to_excel(connection_string, database_name, queries, output_file):
    client = MongoClient(connection_string)
    db = client[database_name]

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        for query_name, query in queries.items():
            collection_name = query["collection"]
            filter_query = query.get("filter", {})
            projection = query.get("projection", None)

            print(f"▶ Running query '{query_name}' on collection '{collection_name}'")

            cursor = db[collection_name].find(filter_query, projection)
            docs = list(cursor)

            if docs:
                # Flatten deeply nested JSON + arrays
                df = flatten_documents(docs)

                # Limit sheet name length to 31 chars
                sheet_name = query_name[:31]
                df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"✅ Excel file created: {output_file}")


# -------------------------------
# Example usage
# -------------------------------

if __name__ == "__main__":
    connection_string = "ADD_CONN_STRING_HERE"
    database_name = "uatdomainmodeladobesandbox"

    # Define multiple queries across multiple collections
    queries = {
        "Supplier Risk Assessment": {
            "collection": "riskAssessment_1664901704",
            "filter": { "isDeleted":False,"riskAssessmentType.code": "3","bulkprocessId":"3c417292-05b8-43da-9431-76959cbb8f3d" },
            "projection": {
                "documentNumber": 1,
                "basicDetail.businessUnit.entityName": 1,
                "basicDetail.businessUnit.entityDetailCode": 1,
                "basicDetail.businessUnit.level": 1,
                "basicDetail.category.id": 1,
                "basicDetail.category.clientCode": 1,
                "basicDetail.category.name": 1,
                "basicDetail.category.level" : 1,
                "basicDetail.region.name": 1,
                "basicDetail.region.id": 1,
                "basicDetail.region.level": 1,
                "supplierId": 1,
                "contractId": 1,
                "contractDocNumber": 1,
                "revisedContractNumber": 1,
                "internalDocumentId": 1,
                "dueDiligencePhase": 1
            }
        },
         "Key Risk Attributes": {
            "collection": "riskAssessment_1664901704",
            "filter": { "isDeleted": False, "riskAssessmentType.code": {"$in" : ["2","3","4"]},"bulkprocessId":"3c417292-05b8-43da-9431-76959cbb8f3d" },
            "projection": {
                "documentNumber": 1,
                "supplierId": 1,
                "contractId": 1,
                "contractDocNumber": 1,
                "revisedContractNumber": 1,
                "internalDocumentId": 1,
                "dueDiligencePhase": 1,
                "riskAttributeFields":1
            }
        },
          "Risk Characteristics": {
            "collection": "riskAssessment_1664901704",
            "filter": { "isDeleted": False, "riskAssessmentType.code": {"$in" : ["2","3","4"]},"bulkprocessId":"3c417292-05b8-43da-9431-76959cbb8f3d" },
            "projection": {
                "documentNumber": 1,
                "riskProfile.overallScore":1,
                "riskProfile.riskScoreLevel.riskScoreRating":1,
                "riskProfile.characteristicsScore.riskCharacteristics.name":1,
                "riskProfile.characteristicsScore.characteristicScore":1,
                "riskProfile.characteristicsScore.riskCharacteristics.riskCharacteristicsRating.scoreLevel":1,
                "supplierId": 1,
                "contractId": 1,
                "contractDocNumber": 1,
                "revisedContractNumber": 1,
                "internalDocumentId": 1,
                "dueDiligencePhase": 1,
            }
        }
    }

    run_queries_and_export_to_excel(connection_string, database_name, queries, "mongo_extract.xlsx")
