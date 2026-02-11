import xml.etree.ElementTree as ET
from datetime import datetime, timezone

ns = {"atom": "http://www.w3.org/2005/Atom", "cust": "http://naesb.org/espi/customer"}


def parse_customer_info(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Collect agreements (agreement_id -> {name, sign_date})
    agreements = {}
    # Collect service locations (agreement_id -> location info)
    locations = {}
    # Collect accounts (account_id -> account name)
    accounts = {}

    for entry in root.findall("atom:entry", ns):
        content = entry.find("atom:content", ns)
        if content is None:
            continue

        # Extract the self link for ID parsing
        self_link = None
        for link in entry.findall("atom:link", ns):
            if link.get("rel") == "self":
                self_link = link.get("href")

        # --- CustomerAccount ---
        acct = content.find("cust:CustomerAccount", ns)
        if acct is not None and self_link:
            parts = self_link.split("/")
            acct_id = parts[parts.index("CustomerAccount") + 1]
            accounts[acct_id] = acct.findtext("cust:name", namespaces=ns)

        # --- CustomerAgreement ---
        agree = content.find("cust:CustomerAgreement", ns)
        if agree is not None and self_link:
            parts = self_link.split("/")
            agree_id = parts[parts.index("CustomerAgreement") + 1]
            acct_id = parts[parts.index("CustomerAccount") + 1]
            sign_ts = agree.findtext("cust:signDate", namespaces=ns)
            sign_date = None
            if sign_ts:
                sign_date = datetime.fromtimestamp(
                    int(sign_ts), tz=timezone.utc
                ).strftime("%Y-%m-%d")
            agreements[agree_id] = {
                "agreement_name": agree.findtext("cust:name", namespaces=ns),
                "sign_date": sign_date,
                "account_id": acct_id,
            }

        # --- ServiceLocation ---
        loc = content.find("cust:ServiceLocation", ns)
        if loc is not None and self_link:
            parts = self_link.split("/")
            agree_id = parts[parts.index("CustomerAgreement") + 1]
            acct_id = parts[parts.index("CustomerAccount") + 1]

            addr = loc.find(
                "cust:mainAddress/cust:streetDetail/cust:addressGeneral", ns
            )
            addr2 = loc.find(
                "cust:mainAddress/cust:streetDetail/cust:addressGeneral2", ns
            )
            city = loc.find("cust:mainAddress/cust:townDetail/cust:name", ns)
            state = loc.find(
                "cust:mainAddress/cust:townDetail/cust:stateOrProvince", ns
            )
            zip_code = loc.find("cust:mainAddress/cust:townDetail/cust:code", ns)

            locations[agree_id] = {
                "account_id": acct_id,
                "address": addr.text if addr is not None else None,
                "address2": addr2.text if addr2 is not None else None,
                "city": city.text if city is not None else None,
                "state": state.text if state is not None else None,
                "zip": zip_code.text if zip_code is not None else None,
            }

    # Merge into a flat table
    rows = []
    for agree_id, loc_info in locations.items():
        agree_info = agreements.get(agree_id, {})
        acct_id = loc_info["account_id"]
        rows.append(
            {
                "account_id": acct_id,
                "account_name": accounts.get(acct_id),
                "agreement_id": agree_id,
                "agreement_name": agree_info.get("agreement_name"),
                "sign_date": agree_info.get("sign_date"),
                "address": loc_info["address"],
                "address2": loc_info["address2"],
                "city": loc_info["city"],
                "state": loc_info["state"],
                "zip": loc_info["zip"],
            }
        )
    return rows


if __name__ == "__main__":
    rows = parse_customer_info("data/cust_info.json")

    # Or load into Polars:
    import polars as pl

    df = pl.from_dicts(rows)
    df.write_csv("cust_info.csv")
    print(df)
