import xml.etree.ElementTree as ET
import polars as pl

ns = {"atom": "http://www.w3.org/2005/Atom", "espi": "http://naesb.org/espi"}


def parse_xml(xml_file):
    """
    Parses a simple XML file and returns a list of dictionaries.
    Assumes a structure like: <root> <row> <col1>data</col1> ... </row> </root>
    """
    tree = ET.parse(xml_file)
    root = tree.getroot()
    data_list = []

    for child in root.findall("atom:entry", ns):
        content = child.find("atom:content", ns)
        data = content.find(".//espi:IntervalBlock", ns)
        if data is None:
            continue
        usage_point_id = extract_usage_point_id(child)
        for reading in data.findall("espi:IntervalReading", ns):
            row = {
                "usage_point": usage_point_id,
                "reading_quality": reading.findtext(
                    "espi:ReadingQuality/espi:quality", default=None, namespaces=ns
                ),
                "duration": reading.findtext(
                    "espi:timePeriod/espi:duration", default=None, namespaces=ns
                ),
                "start": reading.findtext(
                    "espi:timePeriod/espi:start", default=None, namespaces=ns
                ),
                "value": reading.findtext("espi:value", default=None, namespaces=ns),
                "tou": reading.findtext("espi:tou", default=None, namespaces=ns),
            }
            data_list.append(row)
    return data_list


def extract_usage_point_id(child):
    for link in child.findall("atom:link", ns):
        if link.get("rel") == "up":
            ref_split = link.get("href").split("/")
            return ref_split[(ref_split.index("UsagePoint") + 1)]
        else:
            continue
