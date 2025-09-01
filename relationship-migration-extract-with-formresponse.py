import requests
import pandas as pd
from pymongo import MongoClient
from collections.abc import MutableMapping
from datetime import datetime
import argparse

parser = argparse.ArgumentParser(description="Example script with parameters")

# Flags
parser.add_argument("--fetchAll", action="store_true", help="Fetch all data")
parser.add_argument("--fetchDeleted", action="store_true", help="Fetch deleted data")

args = parser.parse_args()

# Usage
print("fetchAll =", args.fetchAll)
print("fetchDeleted =", args.fetchDeleted)
# -------------------------------
# Existing flatten helpers (same as yours)
# -------------------------------
def flatten_dict(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            items.append((new_key, v))
        else:
            items.append((new_key, v))
    return dict(items)

def flatten_documents(docs):
    for d in docs:
        for k, v in d.items():
            if not isinstance(v, (str, int, float, bool, type(None), list, dict)):
                d[k] = str(v)

    flat_docs = [flatten_dict(d) for d in docs]
    df = pd.DataFrame(flat_docs)

    array_cols = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, list)).any()]
    for col in array_cols:
        df = df.explode(col, ignore_index=True)
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            expanded = pd.json_normalize(df[col]).add_prefix(f"{col}.")
            df = df.drop(columns=[col]).reset_index(drop=True).join(expanded)

    return df

# -------------------------------
# New: Fetch API Data
# -------------------------------
def fetch_api_data(internal_document_ids):
    # 1. Get token
    token_resp = requests.post(
        "https://api-smartuat.gep.com/SmartInterfaceAPI/api/Common/GetToken",
        json={"bpc": 70022705}
    )
    token_resp.raise_for_status()
    token = token_resp.json()["token"]

    # 2. Call Run API
    headers = {"Authorization": f"Bearer {token}"}
    payload = {
        "ClientId": "70022705",
        "AppId": "1090",
        "PluginId": "1",
        "PluginVersion": "1",
        "OperationName": "FetchResponseExtract",
        "Version": "1",
        "Variables": {"internalDocumentIds": internal_document_ids},
        "TransactionId": "955806cc-e968-44af-a766-ca58c79ab538",
        "IsRetry": True,
        "DacThumbprint": None,
        "QueryResolverSettings": {
            "BaseUrl": None,
            "AcsAppClientId": None,
            "JWToken": None,
            "TransactionScopeId": None
        }
    }
    run_resp = requests.post(
        "https://leoaksuat.gep.com/leo-storage-dataservice/api/v1/StorageService/Run",
        headers=headers, json=payload
    )
    run_resp.raise_for_status()
    data = run_resp.json().get("ouputData", [])
    return pd.DataFrame(data) if data else pd.DataFrame()

# -------------------------------
# Existing: Mongo + Excel export
# -------------------------------
def run_queries_and_export_to_excel(connection_string, database_name, queries, output_file):
    client = MongoClient(connection_string)
    db = client[database_name]

    internal_document_ids = set()  # Collect IDs from Forms

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        # 1. Mongo queries
        for query_name, query in queries.items():
            collection_name = query["collection"]
            filter_query = query.get("filter", {})
            projection = query.get("projection", None)

            print(f"▶ Running query '{query_name}' on collection '{collection_name}'")

            if args.fetchDeleted:
                filter_query["isDeleted"] = {"$in": [True, False]}
            else:
                filter_query["isDeleted"] = False
            cursor = db[collection_name].find(filter_query, projection)
            docs = list(cursor)

            if docs:
                df = flatten_documents(docs)
                sheet_name = query_name[:31]
                df.to_excel(writer, sheet_name=sheet_name, index=False)

                # Collect internalDocumentId from Forms query
                if query_name == "Forms":
                    internal_document_ids.update(df["internalDocumentId"].dropna().unique())

        # 2. API response sheet
        if internal_document_ids:
            print(f"▶ Fetching API data for {len(internal_document_ids)} internalDocumentIds...")
            api_df = fetch_api_data(list(internal_document_ids))
            if not api_df.empty:
                api_df.to_excel(writer, sheet_name="ResponseExtract", index=False)

    print(f"✅ Excel file created: {output_file}")


# -------------------------------
# Example usage
# -------------------------------
if __name__ == "__main__":
    connection_string = "mongodb+srv://uatleoustenantdocteamsro:900aV2aJEQkF9p2W@uat-us-leo-tenant-2.cnkwb.mongodb.net/?ssl=true&authSource=admin&retryWrites=true&readPreference=secondary&readPreferenceTags=nodeType:ANALYTICS&w=majority&wtimeoutMS=5000&readConcernLevel=majority&retryReads=true&appName=docteamro"
    database_name = "uatdomainmodeladobesandbox"
    if args.fetchAll:
        bulkProcessIds = {"$nin" : ["",None]}
    else :
        bulkProcessIds = {"$in": ["76d6d840-ab60-4384-9c5e-b1e9f918de74"]}
    queries = {
    "Supplier Risk Assessment": {
        "collection": "riskAssessment_1664901704",
        "filter": {
            "riskAssessmentType.code": "3",
            "bulkprocessId": bulkProcessIds
        },
        "projection": {
            "documentNumber": 1,
            "basicDetail.businessUnit.entityName": 1,
            "basicDetail.businessUnit.entityDetailCode": 1,
            "basicDetail.businessUnit.level": 1,
            "basicDetail.category.id": 1,
            "basicDetail.category.clientCode": 1,
            "basicDetail.category.name": 1,
            "basicDetail.category.level": 1,
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
        "filter": {
            "riskAssessmentType.code": {"$in": ["2", "3", "4"]},
            "bulkprocessId": bulkProcessIds,
        },
        "projection": {
            "documentNumber": 1,
            "supplierId": 1,
            "contractId": 1,
            "contractDocNumber": 1,
            "revisedContractNumber": 1,
            "internalDocumentId": 1,
            "dueDiligencePhase": 1,
            "riskAttributeFields": 1
        }
    },
    "Risk Characteristics": {
        "collection": "riskAssessment_1664901704",
        "filter": {
            "riskAssessmentType.code": {"$in": ["2", "3", "4"]},
            "bulkprocessId": bulkProcessIds
        },
        "projection": {
            "documentNumber": 1,
            "riskProfile.overallScore": 1,
            "riskProfile.riskScoreLevel.riskScoreRating": 1,
            "riskProfile.characteristicsScore.riskCharacteristics.name": 1,
            "riskProfile.characteristicsScore.characteristicScore": 1,
            "riskProfile.characteristicsScore.riskCharacteristics.riskCharacteristicsRating.scoreLevel": 1,
            "supplierId": 1,
            "contractId": 1,
            "contractDocNumber": 1,
            "revisedContractNumber": 1,
            "internalDocumentId": 1,
            "dueDiligencePhase": 1
        }
    },
    "Forms": {
        "collection": "form_1663277990",
        "filter": {"bulkprocessId": bulkProcessIds },
        "projection": {
            "sourceFormDocumentNumber": 1,
            "dueDilligencePhase": 1,
            "responderInformation": 1,
            "additionalDetails.launchDate": 1,
            "additionalDetails.firstCompletionDate": 1,
            "sourceFormInternalDocId": 1,
            "supplierRSAId": 1,
            "taskDetails": 1,
            "bulkprocessId": 1,
            "internalDocumentId" : 1
        }
    },
    "Recurrence": {
        "collection": "manageRecurrence_1671208142",
        "filter": {"bulkprocessId": bulkProcessIds },
        "projection": {
            "documentNumber": 1,
            "source": 1,
            "recurrenceCycleDetails.nextLaunchDate": 1,
            "recurrenceDetails.recurrenceFromOcurrence": 1,
            "recurrenceDetails.recurrenceType": 1,
            "masterFormInternalDocumentId": 1,
            "masterFormName": 1,
            "manageRecurrenceId": 1
        }
    },
    "Relationship with KRA": {
        "collection": "relationship_1667773947",
        "filter": {"bulkProcessId": bulkProcessIds },
        "projection": {
            "documentNumber": 1,
            "supplierId": 1,
            "supplierName": 1,
            "relationshipType": 1,
            "customAttributeFields" : 1
        }
    }
}

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"RELATIONSHIP_MIGRATION_EXTRACT_{timestamp}.xlsx"
    run_queries_and_export_to_excel(connection_string, database_name, queries, output_file)
